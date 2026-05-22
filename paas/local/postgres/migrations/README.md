# Local Migrations

Put optional `*.sql` migration files here while developing locally. They run
in sorted filename order the first time the Docker volume is initialized.

Use `palimpsest dev reset` to recreate the local database and rerun them.
