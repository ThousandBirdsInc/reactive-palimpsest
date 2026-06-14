// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{
    collections::BTreeSet,
    fs,
    net::SocketAddr,
    path::PathBuf,
    process::ExitCode,
    sync::{Arc, Mutex},
    time::Duration,
};

use palimpsest_paas_control_plane::{
    control_plane_router, run_acme_order_scheduler_once, run_backup_scheduler_once,
    run_maintenance_scheduler_once, run_pitr_check_scheduler_once,
    run_restore_drill_scheduler_once, sql_control_plane_router, sql_store, ControlPlaneService,
    HostCapacity, Reconciler,
};
use palimpsest_paas_core::{ManagedPostgresCluster, PostgresVersion};

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [command] if command == "help" || command == "--help" || command == "-h" => {
            print_help();
            Ok(())
        }
        [command, addr] if command == "serve-api" => {
            let addr: SocketAddr = addr
                .parse()
                .map_err(|err| format!("parse listen addr '{addr}': {err}"))?;
            let service = Arc::new(Mutex::new(ControlPlaneService::default()));
            let app = control_plane_router(service);
            let listener = tokio::net::TcpListener::bind(addr)
                .await
                .map_err(|err| format!("bind {addr}: {err}"))?;
            println!("palimpsest PaaS control-plane API listening on {addr}");
            axum::serve(listener, app)
                .await
                .map_err(|err| format!("serve API: {err}"))
        }
        [command, addr, database_url] if command == "serve-sql-api" => {
            let addr: SocketAddr = addr
                .parse()
                .map_err(|err| format!("parse listen addr '{addr}': {err}"))?;
            let pool = sql_store::connect(database_url)
                .await
                .map_err(|err| err.to_string())?;
            let secret_backend =
                sql_store::SecretMaterialBackend::from_env().map_err(|err| err.to_string())?;
            let store = Arc::new(sql_store::SqlControlPlaneStore::with_secret_backend(
                pool,
                secret_backend,
            ));
            if let Some(interval) = backup_scheduler_interval()? {
                spawn_backup_scheduler(Arc::clone(&store), interval);
            }
            if let Some(config) = restore_drill_scheduler_config()? {
                spawn_restore_drill_scheduler(
                    Arc::clone(&store),
                    config.interval,
                    config.max_age_hours,
                );
            }
            if let Some(config) = pitr_check_scheduler_config()? {
                spawn_pitr_check_scheduler(Arc::clone(&store), config.interval, config.max_age_hours);
            }
            if let Some(interval) = acme_order_scheduler_interval()? {
                spawn_acme_order_scheduler(Arc::clone(&store), interval);
            }
            if let Some(config) = maintenance_scheduler_config()? {
                spawn_maintenance_scheduler(
                    Arc::clone(&store),
                    config.interval,
                    config.target_postgres_version,
                );
            }
            let app = sql_control_plane_router(store);
            let listener = tokio::net::TcpListener::bind(addr)
                .await
                .map_err(|err| format!("bind {addr}: {err}"))?;
            println!("palimpsest PaaS SQL control-plane API listening on {addr}");
            axum::serve(listener, app)
                .await
                .map_err(|err| format!("serve SQL API: {err}"))
        }
        [command, cluster_path] if command == "plan" => {
            let cluster = read_cluster(cluster_path)?;
            let hosts = [HostCapacity {
                host_id: "local-dev-host".to_owned(),
                data_root: "/var/lib/palimpsest/postgres".to_owned(),
                first_port: 55_000,
                max_clusters: 16,
                assigned_clusters: 0,
                used_ports: BTreeSet::new(),
                storage_gib: 1024,
                used_storage_gib: 0,
            }];
            let plan = Reconciler::new()
                .reconcile(&cluster, &hosts)
                .map_err(|err| err.to_string())?;
            println!(
                "{}",
                serde_json::to_string_pretty(&plan.commands).map_err(|err| err.to_string())?
            );
            Ok(())
        }
        [] => {
            print_help();
            Ok(())
        }
        _ => Err(
            "usage: palimpsest-paas-control-plane plan <cluster.json> | serve-api <addr> | serve-sql-api <addr> <postgres-url>"
                .to_owned(),
        ),
    }
}

fn backup_scheduler_interval() -> Result<Option<Duration>, String> {
    scheduler_interval_from_raw(
        "PALIMPSEST_PAAS_BACKUP_SCHEDULER_INTERVAL_SECONDS",
        std::env::var("PALIMPSEST_PAAS_BACKUP_SCHEDULER_INTERVAL_SECONDS").ok(),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecoverabilitySchedulerConfig {
    interval: Duration,
    max_age_hours: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MaintenanceSchedulerConfig {
    interval: Duration,
    target_postgres_version: PostgresVersion,
}

fn restore_drill_scheduler_config() -> Result<Option<RecoverabilitySchedulerConfig>, String> {
    recoverability_scheduler_config(
        "PALIMPSEST_PAAS_RESTORE_DRILL_SCHEDULER_INTERVAL_SECONDS",
        std::env::var("PALIMPSEST_PAAS_RESTORE_DRILL_SCHEDULER_INTERVAL_SECONDS").ok(),
        "PALIMPSEST_PAAS_RESTORE_DRILL_MAX_AGE_HOURS",
        std::env::var("PALIMPSEST_PAAS_RESTORE_DRILL_MAX_AGE_HOURS").ok(),
    )
}

fn pitr_check_scheduler_config() -> Result<Option<RecoverabilitySchedulerConfig>, String> {
    recoverability_scheduler_config(
        "PALIMPSEST_PAAS_PITR_CHECK_SCHEDULER_INTERVAL_SECONDS",
        std::env::var("PALIMPSEST_PAAS_PITR_CHECK_SCHEDULER_INTERVAL_SECONDS").ok(),
        "PALIMPSEST_PAAS_PITR_CHECK_MAX_AGE_HOURS",
        std::env::var("PALIMPSEST_PAAS_PITR_CHECK_MAX_AGE_HOURS").ok(),
    )
}

fn acme_order_scheduler_interval() -> Result<Option<Duration>, String> {
    scheduler_interval_from_raw(
        "PALIMPSEST_PAAS_ACME_ORDER_SCHEDULER_INTERVAL_SECONDS",
        std::env::var("PALIMPSEST_PAAS_ACME_ORDER_SCHEDULER_INTERVAL_SECONDS").ok(),
    )
}

fn maintenance_scheduler_config() -> Result<Option<MaintenanceSchedulerConfig>, String> {
    let Some(interval) = scheduler_interval_from_raw(
        "PALIMPSEST_PAAS_MAINTENANCE_SCHEDULER_INTERVAL_SECONDS",
        std::env::var("PALIMPSEST_PAAS_MAINTENANCE_SCHEDULER_INTERVAL_SECONDS").ok(),
    )?
    else {
        return Ok(None);
    };
    let raw = std::env::var("PALIMPSEST_PAAS_MAINTENANCE_TARGET_POSTGRES_VERSION").map_err(|_| {
        "PALIMPSEST_PAAS_MAINTENANCE_TARGET_POSTGRES_VERSION is required when maintenance scheduler is enabled"
            .to_owned()
    })?;
    let target_postgres_version = PostgresVersion::new(raw).map_err(|err| err.to_string())?;
    Ok(Some(MaintenanceSchedulerConfig {
        interval,
        target_postgres_version,
    }))
}

fn recoverability_scheduler_config(
    interval_var: &str,
    interval_raw: Option<String>,
    max_age_var: &str,
    max_age_raw: Option<String>,
) -> Result<Option<RecoverabilitySchedulerConfig>, String> {
    let Some(interval) = scheduler_interval_from_raw(interval_var, interval_raw)? else {
        return Ok(None);
    };
    Ok(Some(RecoverabilitySchedulerConfig {
        interval,
        max_age_hours: positive_i64_from_raw(max_age_var, max_age_raw)?,
    }))
}

fn scheduler_interval_from_raw(
    var_name: &str,
    raw: Option<String>,
) -> Result<Option<Duration>, String> {
    let Some(raw) = raw.filter(|value| !value.trim().is_empty()) else {
        return Ok(None);
    };
    let seconds = raw
        .parse::<u64>()
        .map_err(|err| format!("parse {var_name} '{raw}': {err}"))?;
    if seconds == 0 {
        Ok(None)
    } else {
        Ok(Some(Duration::from_secs(seconds)))
    }
}

fn positive_i64_from_raw(var_name: &str, raw: Option<String>) -> Result<Option<i64>, String> {
    let Some(raw) = raw.filter(|value| !value.trim().is_empty()) else {
        return Ok(None);
    };
    let value = raw
        .parse::<i64>()
        .map_err(|err| format!("parse {var_name} '{raw}': {err}"))?;
    if value < 1 {
        return Err(format!("{var_name} must be at least 1 hour when set"));
    }
    Ok(Some(value))
}

fn spawn_backup_scheduler(store: Arc<sql_store::SqlControlPlaneStore>, interval: Duration) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            match run_backup_scheduler_once(&store, "backup-scheduler").await {
                Ok(result) if result.scheduled.is_empty() => {}
                Ok(result) => {
                    eprintln!(
                        "backup scheduler queued {} managed Postgres backup(s)",
                        result.scheduled.len()
                    );
                }
                Err(err) => {
                    eprintln!("backup scheduler run failed: {err:?}");
                }
            }
        }
    });
}

fn spawn_restore_drill_scheduler(
    store: Arc<sql_store::SqlControlPlaneStore>,
    interval: Duration,
    max_age_hours: Option<i64>,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            match run_restore_drill_scheduler_once(&store, "restore-drill-scheduler", max_age_hours)
                .await
            {
                Ok(result) if result.scheduled.is_empty() => {}
                Ok(result) => {
                    eprintln!(
                        "restore drill scheduler queued {} managed Postgres restore drill(s)",
                        result.scheduled.len()
                    );
                }
                Err(err) => {
                    eprintln!("restore drill scheduler run failed: {err:?}");
                }
            }
        }
    });
}

fn spawn_pitr_check_scheduler(
    store: Arc<sql_store::SqlControlPlaneStore>,
    interval: Duration,
    max_age_hours: Option<i64>,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            match run_pitr_check_scheduler_once(&store, "pitr-check-scheduler", max_age_hours).await
            {
                Ok(result) if result.checked.is_empty() => {}
                Ok(result) => {
                    eprintln!(
                        "PITR check scheduler ran {} managed Postgres PITR check(s)",
                        result.checked.len()
                    );
                }
                Err(err) => {
                    eprintln!("PITR check scheduler run failed: {err:?}");
                }
            }
        }
    });
}

fn spawn_acme_order_scheduler(store: Arc<sql_store::SqlControlPlaneStore>, interval: Duration) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            match run_acme_order_scheduler_once(&store, "acme-order-scheduler", None).await {
                Ok(response) => {
                    if !response.checked.is_empty() {
                        println!(
                            "ACME order scheduler validated {} managed Postgres order(s)",
                            response.checked.len()
                        );
                    }
                }
                Err(err) => {
                    eprintln!("ACME order scheduler run failed: {err:?}");
                }
            }
        }
    });
}

fn spawn_maintenance_scheduler(
    store: Arc<sql_store::SqlControlPlaneStore>,
    interval: Duration,
    target_postgres_version: PostgresVersion,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            match run_maintenance_scheduler_once(
                &store,
                "maintenance-scheduler",
                target_postgres_version.clone(),
                None,
                None,
            )
            .await
            {
                Ok(result) if result.scheduled.is_empty() => {}
                Ok(result) => {
                    eprintln!(
                        "maintenance scheduler queued {} managed Postgres minor update(s)",
                        result.scheduled.len()
                    );
                }
                Err(err) => {
                    eprintln!("maintenance scheduler run failed: {err:?}");
                }
            }
        }
    });
}

fn read_cluster(path: &str) -> Result<ManagedPostgresCluster, String> {
    let path = PathBuf::from(path);
    let raw = fs::read_to_string(&path).map_err(|err| format!("read {}: {err}", path.display()))?;
    serde_json::from_str(&raw).map_err(|err| format!("parse {}: {err}", path.display()))
}

fn print_help() {
    println!(
        "palimpsest-paas-control-plane\n\n\
         Usage:\n  palimpsest-paas-control-plane plan <cluster.json>\n  palimpsest-paas-control-plane serve-api <addr>\n  palimpsest-paas-control-plane serve-sql-api <addr> <postgres-url>\n\n\
         Commands:\n  plan <cluster.json>             Render node-agent commands for one cluster\n  serve-api <addr>                Start the in-memory control-plane HTTP API\n  serve-sql-api <addr> <url>      Start the SQL-backed control-plane HTTP API\n  help                            Show this message\n\n\
         Environment:\n  PALIMPSEST_PAAS_BACKUP_SCHEDULER_INTERVAL_SECONDS enables the SQL backup scheduler loop when set to a positive number of seconds\n  PALIMPSEST_PAAS_RESTORE_DRILL_SCHEDULER_INTERVAL_SECONDS enables the SQL restore-drill scheduler loop\n  PALIMPSEST_PAAS_RESTORE_DRILL_MAX_AGE_HOURS overrides the restore-drill freshness window\n  PALIMPSEST_PAAS_PITR_CHECK_SCHEDULER_INTERVAL_SECONDS enables the SQL PITR-check scheduler loop\n  PALIMPSEST_PAAS_PITR_CHECK_MAX_AGE_HOURS overrides the PITR-check freshness window\n  PALIMPSEST_PAAS_ACME_ORDER_SCHEDULER_INTERVAL_SECONDS enables the SQL ACME order scheduler loop\n  PALIMPSEST_PAAS_MAINTENANCE_SCHEDULER_INTERVAL_SECONDS enables the SQL maintenance scheduler loop\n  PALIMPSEST_PAAS_MAINTENANCE_TARGET_POSTGRES_VERSION sets the PostgreSQL 18+ minor target for auto updates"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheduler_interval_is_disabled_when_absent_empty_or_zero() {
        assert_eq!(
            scheduler_interval_from_raw("TEST_INTERVAL", None).unwrap(),
            None
        );
        assert_eq!(
            scheduler_interval_from_raw("TEST_INTERVAL", Some(String::new())).unwrap(),
            None
        );
        assert_eq!(
            scheduler_interval_from_raw("TEST_INTERVAL", Some("0".to_owned())).unwrap(),
            None
        );
    }

    #[test]
    fn recoverability_scheduler_config_parses_interval_and_freshness_window() {
        let config = recoverability_scheduler_config(
            "TEST_INTERVAL",
            Some("30".to_owned()),
            "TEST_MAX_AGE",
            Some("168".to_owned()),
        )
        .unwrap()
        .expect("scheduler enabled");
        assert_eq!(config.interval, Duration::from_secs(30));
        assert_eq!(config.max_age_hours, Some(168));
    }

    #[test]
    fn recoverability_scheduler_config_rejects_invalid_freshness_window() {
        let err = recoverability_scheduler_config(
            "TEST_INTERVAL",
            Some("30".to_owned()),
            "TEST_MAX_AGE",
            Some("0".to_owned()),
        )
        .expect_err("zero age is rejected");
        assert!(err.contains("TEST_MAX_AGE"));
    }
}
