Catalog fixtures are regenerated with:

```sh
PG16_DATABASE_URL=postgres://... PG17_DATABASE_URL=postgres://... cargo run -p xtask -- regen-pg-fixtures
```

The generated `catalog_pg16.tsv` and `catalog_pg17.tsv` files use the same column order as `mock_postgres::CatalogProbeFixture`.
