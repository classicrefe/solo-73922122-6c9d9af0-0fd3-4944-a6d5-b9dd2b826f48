-- The source database business tables:
--   1. minute counts (the table restored by PITR);
--   2. the ingestion ledger for out-of-order collector retransmissions.
--
-- traffic_ingest_observations keeps every accepted observation keyed by
-- (intersection_id, seq); traffic_ingest_watermarks holds the highest
-- contiguous seq already published to traffic_minute_counts.  Records above
-- the watermark stay pending until every gap below them is filled.
CREATE TABLE IF NOT EXISTS traffic_minute_counts (
    intersection_id text        NOT NULL,
    minute_utc      timestamptz NOT NULL,
    vehicle_count   integer     NOT NULL CHECK (vehicle_count >= 0),
    PRIMARY KEY (intersection_id, minute_utc)
);

CREATE TABLE IF NOT EXISTS traffic_ingest_observations (
    intersection_id text        NOT NULL,
    seq             bigint      NOT NULL CHECK (seq >= 1),
    minute_utc      timestamptz NOT NULL,
    vehicle_count   integer     NOT NULL CHECK (vehicle_count >= 0),
    received_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (intersection_id, seq)
);

CREATE TABLE IF NOT EXISTS traffic_ingest_watermarks (
    intersection_id text        PRIMARY KEY,
    published_seq   bigint      NOT NULL DEFAULT 0 CHECK (published_seq >= 0),
    updated_at      timestamptz NOT NULL DEFAULT now()
);
