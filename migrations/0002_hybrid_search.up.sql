-- 0002_hybrid_search.up.sql
--
-- Supports the keyword half of `search_mentions` hybrid retrieval
-- (pkg/queries/hybrid_search.go). The vector half is already served by
-- mention_embeddings_hnsw from 0001.

-- GIN trigram index over mention text. Backs both the `%` (similarity) and
-- `<%` (word_similarity) operators used by the keyword retrieval pass.
CREATE INDEX IF NOT EXISTS mentions_text_trgm
    ON mentions USING gin (text gin_trgm_ops);

-- The keyword pass filters on created_at for the `since` parameter, and the
-- trending/timeseries queries scan by time. TimescaleDB already partitions on
-- created_at, but an explicit DESC index keeps "recent mentions" cheap.
CREATE INDEX IF NOT EXISTS mentions_created_at_desc_idx
    ON mentions (created_at DESC);
