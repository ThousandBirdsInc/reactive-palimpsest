#!/usr/bin/env bash
set -euo pipefail

run_sql_dir() {
  local dir="$1"
  if [ ! -d "$dir" ]; then
    return 0
  fi

  find "$dir" -maxdepth 1 -type f -name '*.sql' | sort | while read -r sql_file; do
    echo "running local SQL: $sql_file"
    psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" --file "$sql_file"
  done
}

run_sql_dir /docker-entrypoint-initdb.d/migrations
run_sql_dir /docker-entrypoint-initdb.d/seeds
