# Local Seeds

Put optional `*.sql` seed files here while developing locally. They run after
`postgres/migrations/*.sql` the first time the Docker volume is initialized.

Use `palimpsest dev reset` to recreate the local database and rerun them.
