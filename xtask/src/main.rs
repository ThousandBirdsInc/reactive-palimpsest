// Copyright 2026 Thousand Birds Inc.
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{env, process::ExitCode};

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
        "regen-fixtures" => todo_command("regen-fixtures"),
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
         Commands:\n  help\n  regen-fixtures\n  check-wasm-size"
    );
}

fn todo_command(name: &str) -> ExitCode {
    eprintln!("xtask command '{name}' is scaffolded but not implemented yet");
    ExitCode::FAILURE
}
