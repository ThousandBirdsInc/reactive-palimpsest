// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{
    env, fs, io,
    path::Path,
    process::{Command, ExitCode},
};

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

    match command.as_str() {
        "help" | "--help" | "-h" => {
            print_help();
            ExitCode::SUCCESS
        }
        "regen-fixtures" | "regen-pg-fixtures" => regen_pg_fixtures(),
        "check-reference-budget" => check_reference_budget(),
        "check-wasm-size" => todo_command("check-wasm-size"),
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
         Usage:\n  cargo run -p xtask -- <command>\n\n\
         Commands:\n  help\n  regen-fixtures\n  regen-pg-fixtures\n  check-reference-budget\n  check-wasm-size"
    );
}

fn todo_command(name: &str) -> ExitCode {
    eprintln!("xtask command '{name}' is scaffolded but not implemented yet");
    ExitCode::FAILURE
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

    Ok(())
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
