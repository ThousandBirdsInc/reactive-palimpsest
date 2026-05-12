// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{
    env,
    fmt::Write as FmtWrite,
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
    process::{Command, ExitCode},
};

use flate2::{write::GzEncoder, Compression};

const CATALOG_QUERY: &str = "\
SELECT c.oid AS relid,
       n.nspname AS namespace,
       c.relname,
       c.relreplident,
       a.attnum,
       a.attname,
       a.atttypid,
       a.attnotnull,
       EXISTS (
         SELECT 1
         FROM pg_index i
         WHERE i.indrelid = c.oid
           AND i.indisprimary
           AND a.attnum = ANY(i.indkey)
       ) AS primary_key
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_attribute a ON a.attrelid = c.oid
WHERE c.relkind = 'r'
  AND n.nspname NOT LIKE 'pg_%'
  AND n.nspname <> 'information_schema'
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY n.nspname, c.relname, a.attnum";

fn main() -> ExitCode {
    let Some(command) = env::args().nth(1) else {
        print_help();
        return ExitCode::SUCCESS;
    };

    let rest: Vec<String> = env::args().skip(2).collect();
    match command.as_str() {
        "help" | "--help" | "-h" => {
            print_help();
            ExitCode::SUCCESS
        }
        "regen-fixtures" | "regen-pg-fixtures" => regen_pg_fixtures(),
        "check-reference-budget" => check_reference_budget(),
        "check-wasm-size" => check_wasm_size(),
        "check-coverage" => check_coverage(&rest),
        "check-bench-regression" => check_bench_regression(&rest),
        "render-bench-dashboard" => render_bench_dashboard(&rest),
        unknown => {
            eprintln!("unknown xtask command: {unknown}");
            print_help();
            ExitCode::FAILURE
        }
    }
}

fn print_help() {
    println!(
        "Palimpsest project tasks\n\n\
         Usage:\n  cargo run -p xtask -- <command> [args...]\n\n\
         Commands:\n  help\n  regen-fixtures\n  regen-pg-fixtures\n  check-reference-budget\n  check-wasm-size\n  check-coverage --lcov <path>\n  check-bench-regression --threshold <pct>\n  render-bench-dashboard --input <ndjson> --out <dir>"
    );
}

fn regen_pg_fixtures() -> ExitCode {
    match try_regen_pg_fixtures() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("regen-pg-fixtures failed: {err}");
            ExitCode::FAILURE
        }
    }
}

fn try_regen_pg_fixtures() -> io::Result<()> {
    let output_dir = Path::new("crates/palimpsest-test-harness/fixtures");
    fs::create_dir_all(output_dir)?;

    for (version, env_var) in [("16", "PG16_DATABASE_URL"), ("17", "PG17_DATABASE_URL")] {
        let database_url = env::var(env_var).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{env_var} must point at a Postgres {version} database"),
            )
        })?;
        let output = Command::new("psql")
            .args([
                "--no-psqlrc",
                "--tuples-only",
                "--no-align",
                "--field-separator",
                "\t",
                &database_url,
                "--command",
                CATALOG_QUERY,
            ])
            .output()?;

        if !output.status.success() {
            return Err(io::Error::other(
                String::from_utf8_lossy(&output.stderr).into_owned(),
            ));
        }

        fs::write(
            output_dir.join(format!("catalog_pg{version}.tsv")),
            output.stdout,
        )?;
    }

    // Conformance fixtures (§18.13.4). Delegated to the conformance
    // test binary itself: with `PALIMPSEST_CONFORMANCE_REGEN=1` the
    // catalog_responses test writes JSON to `fixtures/` instead of
    // asserting. Skipped if `PALIMPSEST_PG_URL` is not set, since
    // there is no live database to talk to.
    if env::var("PALIMPSEST_PG_URL").is_ok() {
        let status = Command::new(env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
            .args([
                "test",
                "-p",
                "palimpsest-conformance",
                "--features",
                "real-postgres",
                "--test",
                "catalog_responses",
                "--",
                "--nocapture",
            ])
            .env("PALIMPSEST_CONFORMANCE_REGEN", "1")
            .status()?;
        if !status.success() {
            return Err(io::Error::other(format!(
                "conformance fixture regen failed (status {status:?})"
            )));
        }
    } else {
        eprintln!("regen-pg-fixtures: PALIMPSEST_PG_URL unset; skipping conformance fixtures");
    }

    Ok(())
}

/// Build `palimpsest-client-js` for `wasm32-unknown-unknown` under
/// `--profile release-wasm`, then assert the gzipped `.wasm` is under
/// the budget. The budget is read from `PALIMPSEST_WASM_SIZE_BUDGET`
/// (in bytes) or defaults to 500 KB per §18.11.
#[allow(clippy::cast_precision_loss)]
fn check_wasm_size() -> ExitCode {
    let budget_bytes = env::var("PALIMPSEST_WASM_SIZE_BUDGET")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(500 * 1024);

    let mut args = vec![
        "build",
        "-p",
        "palimpsest-client-js",
        "--target",
        "wasm32-unknown-unknown",
        "--profile",
        "release-wasm",
    ];
    let extra_features = env::var("PALIMPSEST_WASM_FEATURES").ok();
    if let Some(feats) = extra_features.as_deref() {
        if !feats.is_empty() {
            args.extend_from_slice(&["--features", feats]);
        }
    }
    eprintln!("$ cargo {}", args.join(" "));
    let status = Command::new(env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
        .args(&args)
        .status();
    match status {
        Ok(s) if s.success() => {}
        Ok(s) => {
            eprintln!("cargo build failed with status {s:?}");
            return ExitCode::FAILURE;
        }
        Err(err) => {
            eprintln!("failed to spawn cargo: {err}");
            return ExitCode::FAILURE;
        }
    }

    let wasm_path =
        PathBuf::from("target/wasm32-unknown-unknown/release-wasm/palimpsest_client_js.wasm");
    let bytes = match fs::read(&wasm_path) {
        Ok(b) => b,
        Err(err) => {
            eprintln!("could not read {}: {err}", wasm_path.display());
            return ExitCode::FAILURE;
        }
    };
    let raw_size = bytes.len();
    let gz_size = match gzip_size(&bytes) {
        Ok(n) => n,
        Err(err) => {
            eprintln!("gzip failed: {err}");
            return ExitCode::FAILURE;
        }
    };

    let pct = (gz_size as f64 / budget_bytes as f64) * 100.0;
    println!(
        "palimpsest_client_js.wasm: raw {raw_size} B ({:.1} KB), gz {gz_size} B ({:.1} KB) — {pct:.1}% of {budget_bytes} B budget",
        raw_size as f64 / 1024.0,
        gz_size as f64 / 1024.0,
    );
    if (gz_size as u64) > budget_bytes {
        eprintln!(
            "FAIL: gzipped wasm is over budget ({gz_size} B > {budget_bytes} B). \
             Set PALIMPSEST_WASM_SIZE_BUDGET to override."
        );
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

fn gzip_size(bytes: &[u8]) -> io::Result<usize> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::best());
    encoder.write_all(bytes)?;
    Ok(encoder.finish()?.len())
}

fn check_reference_budget() -> ExitCode {
    match fs::read_to_string("crates/palimpsest-test-harness/src/reference.rs") {
        Ok(contents) if contents.lines().count() <= 1500 => ExitCode::SUCCESS,
        Ok(contents) => {
            eprintln!(
                "reference executor is {} lines; budget is 1500",
                contents.lines().count()
            );
            ExitCode::FAILURE
        }
        Err(err) => {
            eprintln!("failed to read reference executor: {err}");
            ExitCode::FAILURE
        }
    }
}

fn arg_value<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|idx| args.get(idx + 1).map(String::as_str))
}

/// Coverage gate. Reads an LCOV report and asserts the workspace
/// total line coverage is at least `PALIMPSEST_COVERAGE_FLOOR` (default
/// 70%). The floor lives in env so we can ratchet it up without
/// touching code; CI fails loudly when actual < floor.
#[allow(clippy::cast_precision_loss)]
fn check_coverage(args: &[String]) -> ExitCode {
    let lcov_path = arg_value(args, "--lcov").unwrap_or("lcov.info");
    let floor_pct: f64 = env::var("PALIMPSEST_COVERAGE_FLOOR")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(70.0);

    let contents = match fs::read_to_string(lcov_path) {
        Ok(s) => s,
        Err(err) => {
            eprintln!("failed to read {lcov_path}: {err}");
            return ExitCode::FAILURE;
        }
    };

    let mut found = 0_u64;
    let mut hit = 0_u64;
    for line in contents.lines() {
        if let Some(rest) = line.strip_prefix("LF:") {
            found += rest.parse::<u64>().unwrap_or(0);
        } else if let Some(rest) = line.strip_prefix("LH:") {
            hit += rest.parse::<u64>().unwrap_or(0);
        }
    }

    if found == 0 {
        eprintln!("no LF: records in {lcov_path}");
        return ExitCode::FAILURE;
    }
    let pct = (hit as f64 / found as f64) * 100.0;
    println!("workspace line coverage: {hit}/{found} = {pct:.2}% (floor {floor_pct:.2}%)");
    if pct + 1e-6 < floor_pct {
        eprintln!("FAIL: coverage below floor");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

/// Inspect criterion's `target/criterion` reports after a
/// `cargo bench --baseline main` run and fail if any tracked metric
/// regressed by more than `--threshold` percent. We parse the
/// `change/estimates.json` Criterion writes per benchmark.
#[allow(clippy::cast_precision_loss)]
fn check_bench_regression(args: &[String]) -> ExitCode {
    let threshold: f64 = arg_value(args, "--threshold")
        .and_then(|s| s.parse().ok())
        .or_else(|| {
            env::var("PALIMPSEST_BENCH_REGRESSION_PCT")
                .ok()
                .and_then(|s| s.parse().ok())
        })
        .unwrap_or(10.0);
    let root = PathBuf::from("target/criterion");
    if !root.exists() {
        eprintln!(
            "no criterion output at {} — did you run `cargo bench`?",
            root.display()
        );
        return ExitCode::FAILURE;
    }

    let mut regressions: Vec<(String, f64)> = Vec::new();
    walk_criterion(&root, &root, &mut regressions, threshold);
    if regressions.is_empty() {
        println!("no benches regressed beyond {threshold:.1}%");
        ExitCode::SUCCESS
    } else {
        regressions.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        eprintln!(
            "FAIL: {} benches regressed beyond {threshold:.1}%:",
            regressions.len()
        );
        for (name, pct) in &regressions {
            eprintln!("  {name}: +{pct:.1}%");
        }
        ExitCode::FAILURE
    }
}

fn walk_criterion(root: &Path, dir: &Path, out: &mut Vec<(String, f64)>, threshold: f64) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk_criterion(root, &path, out, threshold);
        } else if path.ends_with("change/estimates.json") {
            if let Ok(contents) = fs::read_to_string(&path) {
                if let Some(pct) = parse_mean_pct_change(&contents) {
                    if pct > threshold {
                        let name = path
                            .parent()
                            .and_then(Path::parent)
                            .and_then(|p| p.strip_prefix(root).ok())
                            .map_or_else(
                                || path.display().to_string(),
                                |p| p.display().to_string(),
                            );
                        out.push((name, pct));
                    }
                }
            }
        }
    }
}

/// Pluck `mean.point_estimate` out of criterion's change estimates
/// JSON without pulling a serde dep into xtask. The file is small and
/// machine-generated; a minimal scan is fine.
fn parse_mean_pct_change(json: &str) -> Option<f64> {
    let mean_idx = json.find("\"mean\"")?;
    let after = &json[mean_idx..];
    let pe_idx = after.find("\"point_estimate\"")?;
    let after_pe = &after[pe_idx + "\"point_estimate\"".len()..];
    let colon = after_pe.find(':')?;
    let tail = after_pe[colon + 1..].trim_start();
    let end = tail
        .find(|c: char| {
            !(c.is_ascii_digit() || c == '.' || c == '-' || c == 'e' || c == 'E' || c == '+')
        })
        .unwrap_or(tail.len());
    tail[..end]
        .parse::<f64>()
        .ok()
        .map(|fraction| fraction * 100.0)
}

/// Take a cargo-criterion ndjson stream and emit a tiny static site
/// summarizing each benchmark's mean and confidence interval. Real
/// dashboards add history; this stub just produces a valid HTML report
/// so the workflow has something concrete to publish.
#[allow(clippy::cast_precision_loss)]
fn render_bench_dashboard(args: &[String]) -> ExitCode {
    let input = arg_value(args, "--input").unwrap_or("criterion.ndjson");
    let out_dir = arg_value(args, "--out").unwrap_or("site");

    let contents = match fs::read_to_string(input) {
        Ok(s) => s,
        Err(err) => {
            eprintln!("could not read {input}: {err}");
            return ExitCode::FAILURE;
        }
    };

    let mut rows = String::new();
    for line in contents.lines().filter(|l| !l.trim().is_empty()) {
        if !line.contains("\"reason\":\"benchmark-complete\"") {
            continue;
        }
        let id = extract_string(line, "\"id\"").unwrap_or_else(|| "?".into());
        let mean = extract_nested_number(line, "\"mean\"", "\"estimate\"").unwrap_or(f64::NAN);
        let _ = writeln!(
            rows,
            "<tr><td>{}</td><td>{:.3} ns</td></tr>",
            html_escape(&id),
            mean,
        );
    }
    if let Err(err) = fs::create_dir_all(out_dir) {
        eprintln!("could not create {out_dir}: {err}");
        return ExitCode::FAILURE;
    }
    let html = format!(
        "<!doctype html><meta charset=utf-8><title>Palimpsest benchmarks</title>\n\
         <style>body{{font:14px/1.4 system-ui,sans-serif;max-width:60rem;margin:2rem auto;padding:0 1rem}}\
         th,td{{padding:.25rem .75rem;text-align:left;border-bottom:1px solid #ddd}}\
         th{{background:#f6f6f6}}</style>\n\
         <h1>Palimpsest benchmarks</h1>\n\
         <table><thead><tr><th>Benchmark</th><th>Mean</th></tr></thead><tbody>\n{rows}</tbody></table>\n",
    );
    if let Err(err) = fs::write(format!("{out_dir}/index.html"), html) {
        eprintln!("could not write site: {err}");
        return ExitCode::FAILURE;
    }
    println!("dashboard written to {out_dir}/index.html");
    ExitCode::SUCCESS
}

fn extract_string(line: &str, key: &str) -> Option<String> {
    let idx = line.find(key)?;
    let tail = &line[idx + key.len()..];
    let colon = tail.find(':')?;
    let after = tail[colon + 1..].trim_start();
    let after = after.strip_prefix('"')?;
    let end = after.find('"')?;
    Some(after[..end].to_owned())
}

fn extract_nested_number(line: &str, outer: &str, inner: &str) -> Option<f64> {
    let outer_idx = line.find(outer)?;
    let after_outer = &line[outer_idx..];
    let inner_idx = after_outer.find(inner)?;
    let after_inner = &after_outer[inner_idx + inner.len()..];
    let colon = after_inner.find(':')?;
    let tail = after_inner[colon + 1..].trim_start();
    let end = tail
        .find(|c: char| {
            !(c.is_ascii_digit() || c == '.' || c == '-' || c == 'e' || c == 'E' || c == '+')
        })
        .unwrap_or(tail.len());
    tail[..end].parse::<f64>().ok()
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}
