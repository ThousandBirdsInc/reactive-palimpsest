-- §18.13.5 oracle conformance corpus.
-- The Rust test (oracle.rs) replays an inline corpus today; this file
-- captures the equivalent SQL so reviewers can diff it against any
-- new property-test queries added in `palimpsest-properties/tests`.
--
-- When extending the property-test query corpus, also extend this
-- file *and* the `corpus()` constructor in `tests/oracle.rs`.

DROP TABLE IF EXISTS palimpsest_oracle_posts;
CREATE TABLE palimpsest_oracle_posts (
    id        BIGINT PRIMARY KEY,
    author_id BIGINT NOT NULL,
    score     BIGINT NOT NULL
);

INSERT INTO palimpsest_oracle_posts(id, author_id, score) VALUES
    (1, 7, 10),
    (2, 7, 20),
    (3, 8, 30),
    (4, 8, 40),
    (5, 9, 50);

-- Query corpus (mirrors palimpsest-properties oracle_equivalence.rs):
SELECT id FROM palimpsest_oracle_posts ORDER BY id;
