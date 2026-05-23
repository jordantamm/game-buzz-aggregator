-- 0001_init.up.sql
-- Creates the full schema for the MVP. Run by the `migrate` one-shot container.

CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ─── Canonical games ───────────────────────────────────────────────────
CREATE TABLE games (
    id              TEXT PRIMARY KEY,
    canonical_name  TEXT NOT NULL,
    steam_app_id    INTEGER UNIQUE,
    released_at     DATE,
    metadata        JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX games_canonical_name_trgm
    ON games USING gin (canonical_name gin_trgm_ops);

-- ─── Aliases for entity resolution ─────────────────────────────────────
CREATE TABLE game_aliases (
    game_id     TEXT NOT NULL REFERENCES games(id) ON DELETE CASCADE,
    alias       TEXT NOT NULL,
    source      TEXT NOT NULL,                  -- 'seed' | 'learned'
    confidence  REAL NOT NULL DEFAULT 1.0,
    PRIMARY KEY (game_id, alias)
);

CREATE INDEX game_aliases_alias_trgm
    ON game_aliases USING gin (alias gin_trgm_ops);

-- ─── Mentions (hypertable on created_at) ───────────────────────────────
CREATE TABLE mentions (
    id                  TEXT NOT NULL,
    source              TEXT NOT NULL,
    native_id           TEXT NOT NULL,
    author_hash         TEXT NOT NULL,
    text                TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    engagement          JSONB NOT NULL DEFAULT '{}',
    raw_payload_ref     TEXT,
    parent_id           TEXT,
    source_metadata     JSONB NOT NULL DEFAULT '{}',
    sentiment_score     REAL,
    sentiment_magnitude REAL,
    sentiment_model     TEXT,
    enricher_version    TEXT,
    PRIMARY KEY (id, created_at)
);

-- TimescaleDB requires the partitioning column in the PK; we use a composite PK.
-- The (source, native_id) uniqueness gives us application-level idempotency.
CREATE UNIQUE INDEX mentions_source_native_idx
    ON mentions (source, native_id, created_at);

SELECT create_hypertable('mentions', 'created_at', if_not_exists => TRUE);

CREATE INDEX mentions_source_idx ON mentions (source);

-- ─── Many-to-many mentions ↔ games ─────────────────────────────────────
CREATE TABLE mention_games (
    mention_id  TEXT NOT NULL,
    game_id     TEXT NOT NULL REFERENCES games(id) ON DELETE CASCADE,
    confidence  REAL NOT NULL,
    method      TEXT NOT NULL,
    PRIMARY KEY (mention_id, game_id)
);

CREATE INDEX mention_games_game_id_idx ON mention_games (game_id);

-- ─── Embeddings (HNSW index for vector similarity) ─────────────────────
CREATE TABLE mention_embeddings (
    mention_id      TEXT PRIMARY KEY,
    embedding       vector(384) NOT NULL,
    model_version   TEXT NOT NULL
);

CREATE INDEX mention_embeddings_hnsw
    ON mention_embeddings
    USING hnsw (embedding vector_cosine_ops);

-- ─── Hourly buzz rollup (hypertable) ───────────────────────────────────
CREATE TABLE game_buzz_hourly (
    game_id         TEXT NOT NULL REFERENCES games(id),
    hour            TIMESTAMPTZ NOT NULL,
    mention_count   INTEGER NOT NULL,
    avg_sentiment   REAL,
    PRIMARY KEY (game_id, hour)
);

SELECT create_hypertable('game_buzz_hourly', 'hour', if_not_exists => TRUE);
