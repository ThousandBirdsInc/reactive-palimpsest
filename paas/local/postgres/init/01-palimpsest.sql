CREATE ROLE palimpsest_app LOGIN PASSWORD 'palimpsest_app';
CREATE ROLE palimpsest_repl LOGIN REPLICATION PASSWORD 'palimpsest_repl';

GRANT CONNECT ON DATABASE palimpsest_dev TO palimpsest_app;
GRANT CONNECT ON DATABASE palimpsest_dev TO palimpsest_repl;

CREATE SCHEMA IF NOT EXISTS app AUTHORIZATION palimpsest_app;

CREATE TABLE IF NOT EXISTS app.messages (
    id BIGSERIAL PRIMARY KEY,
    body TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE app.messages OWNER TO palimpsest_app;
GRANT USAGE ON SCHEMA app TO palimpsest_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA app TO palimpsest_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA app TO palimpsest_app;
GRANT USAGE ON SCHEMA app TO palimpsest_repl;
GRANT SELECT ON ALL TABLES IN SCHEMA app TO palimpsest_repl;

CREATE PUBLICATION palimpsest_pub FOR ALL TABLES;

SELECT *
FROM pg_create_logical_replication_slot('palimpsest', 'pgoutput')
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_replication_slots
    WHERE slot_name = 'palimpsest'
);
