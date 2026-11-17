"""Integration tests for ordered batch ingestion against real PostgreSQL.

Skipped unless TRAFFIC_TEST_DSN points at a disposable database, e.g.:

    TRAFFIC_TEST_DSN="host=127.0.0.1 port=5432 user=postgres dbname=traffic_test" \
        python -m pytest tests -q

The tests truncate the three traffic tables before each case, so never point
this at a database whose data you care about.
"""

import os
from datetime import datetime, timedelta, timezone

import psycopg
import pytest
from psycopg.rows import dict_row

from app import config, ingest

DSN = os.environ.get("TRAFFIC_TEST_DSN")

pytestmark = pytest.mark.skipif(not DSN, reason="TRAFFIC_TEST_DSN not set")

BASE_MINUTE = datetime(2026, 11, 17, 9, 0, tzinfo=timezone.utc)


def _truncate():
    conn = psycopg.connect(DSN, autocommit=True)
    try:
        conn.execute(
            "TRUNCATE traffic_minute_counts, traffic_ingest_observations, "
            "traffic_ingest_watermarks"
        )
    finally:
        conn.close()


@pytest.fixture()
def db(monkeypatch):
    """Point the app's DB helpers at the disposable test database."""
    monkeypatch.setattr(config, "PRIMARY_DSN", DSN)
    ingest._schema_ready = False
    _truncate()
    ingest.ensure_schema()
    # NOTE: do not use `with psycopg.connect(...) as conn` here; psycopg3
    # closes the connection when the context manager exits.
    conn = psycopg.connect(DSN, autocommit=True, row_factory=dict_row)
    yield conn
    conn.close()
    _truncate()


def rec(ix, seq, count=None, minutes_offset=None):
    return ingest.IncomingRecord(
        intersection_id=ix,
        seq=seq,
        minute_utc=BASE_MINUTE + timedelta(minutes=(minutes_offset or seq)),
        vehicle_count=seq * 10 if count is None else count,
    )


def rows(conn, ix):
    return [
        (row["minute_utc"], row["vehicle_count"])
        for row in conn.execute(
            "SELECT minute_utc, vehicle_count FROM traffic_minute_counts "
            "WHERE intersection_id = %s ORDER BY minute_utc",
            (ix,),
        ).fetchall()
    ]


def watermark(conn, ix):
    row = conn.execute(
        "SELECT published_seq FROM traffic_ingest_watermarks "
        "WHERE intersection_id = %s",
        (ix,),
    ).fetchone()
    return row["published_seq"] if row else None


def test_gap_records_stay_pending_then_publish_in_order(db):
    result = ingest.submit_batch([rec("A", 1), rec("A", 3), rec("A", 4)])
    assert result["newly_stored"] == 3
    assert watermark(db, "A") == 1
    assert rows(db, "A") == [(BASE_MINUTE + timedelta(minutes=1), 10)]
    pending = ingest.list_pending("A", 100, 0)
    assert pending["next_missing_seq"] == 2
    assert [item["seq"] for item in pending["items"]] == [3, 4]

    result = ingest.submit_batch([rec("A", 2)])
    assert result["intersections"][0]["published_seq"] == 4
    assert result["intersections"][0]["published_now_seqs"] == [2, 3, 4]
    assert [r[0] for r in rows(db, "A")] == [
        BASE_MINUTE + timedelta(minutes=n) for n in (1, 2, 3, 4)
    ]


def test_higher_seq_overrides_same_minute(db):
    # seq 1 and 2 both describe the same minute; seq 2 is the revision.
    ingest.submit_batch(
        [
            ingest.IncomingRecord("A", 1, BASE_MINUTE, 11),
            ingest.IncomingRecord("A", 2, BASE_MINUTE, 99),
        ]
    )
    assert rows(db, "A") == [(BASE_MINUTE, 99)]


def test_revision_already_published_arrives_later(db):
    ingest.submit_batch([ingest.IncomingRecord("A", 1, BASE_MINUTE, 11)])
    ingest.submit_batch([ingest.IncomingRecord("A", 2, BASE_MINUTE, 77)])
    assert rows(db, "A") == [(BASE_MINUTE, 77)]
    assert watermark(db, "A") == 2


def test_same_content_retry_is_idempotent(db):
    batch = [rec("A", 1), rec("A", 2)]
    first = ingest.submit_batch(batch)
    second = ingest.submit_batch(batch)
    assert first["newly_stored"] == 2
    assert second["newly_stored"] == 0
    assert second["already_stored"] == 2
    assert second["intersections"][0]["published_seq"] == 2
    assert len(rows(db, "A")) == 2
    count = db.execute(
        "SELECT count(*) AS n FROM traffic_ingest_observations"
    ).fetchone()["n"]
    assert count == 2


def test_different_content_rejects_whole_batch(db):
    ingest.submit_batch([rec("A", 1)])
    with pytest.raises(ingest.ContentConflict) as excinfo:
        ingest.submit_batch(
            [rec("B", 1), ingest.IncomingRecord("A", 1, BASE_MINUTE, 999)]
        )
    assert excinfo.value.conflicts[0]["intersection_id"] == "A"
    # B's watermark and observation must not exist (whole batch rolled back)
    assert watermark(db, "B") is None
    assert db.execute(
        "SELECT count(*) AS n FROM traffic_ingest_observations "
        "WHERE intersection_id = 'B'"
    ).fetchone()["n"] == 0
    # A's stored value is untouched (rec("A", 1) lives at BASE_MINUTE + 1 min)
    assert rows(db, "A") == [(BASE_MINUTE + timedelta(minutes=1), 10)]


def test_conflict_inside_batch_rejects_everything(db):
    with pytest.raises(ingest.ContentConflict):
        ingest.submit_batch(
            [
                ingest.IncomingRecord("A", 1, BASE_MINUTE, 1),
                ingest.IncomingRecord("A", 1, BASE_MINUTE, 2),
            ]
        )
    assert watermark(db, "A") is None


def test_identical_duplicate_inside_batch_is_collapsed(db):
    result = ingest.submit_batch([rec("A", 1), rec("A", 1), rec("A", 1)])
    assert result["accepted_records"] == 3
    assert result["duplicates_in_batch"] == 2
    assert result["newly_stored"] == 1
    assert watermark(db, "A") == 1


def test_watermark_never_regresses(db):
    ingest.submit_batch([rec("A", 1), rec("A", 2), rec("A", 3)])
    ingest.submit_batch([rec("A", 2)])  # stale retry
    assert watermark(db, "A") == 3
    assert len(rows(db, "A")) == 3


def test_pending_survives_reconnect(db):
    ingest.submit_batch([rec("A", 1), rec("A", 5)])
    # a brand-new connection sees exactly the persisted state
    fresh = psycopg.connect(DSN, autocommit=True)
    try:
        assert fresh.execute(
            "SELECT published_seq FROM traffic_ingest_watermarks "
            "WHERE intersection_id = 'A'"
        ).fetchone()[0] == 1
        assert fresh.execute(
            "SELECT count(*) FROM traffic_ingest_observations "
            "WHERE intersection_id = 'A' AND seq > 1"
        ).fetchone()[0] == 1
    finally:
        fresh.close()


def test_watermark_advance_rolls_back_with_primary_write(db):
    """Inject a primary-table failure: neither rows nor watermark may change."""
    setup = psycopg.connect(DSN, autocommit=True)
    try:
        setup.execute(
            """
            CREATE OR REPLACE FUNCTION test_fail_ix() RETURNS trigger
            LANGUAGE plpgsql AS $$
            BEGIN
              IF NEW.intersection_id = 'FAILIX' THEN
                RAISE EXCEPTION 'injected failure';
              END IF;
              RETURN NEW;
            END;
            $$;
            """
        )
        setup.execute(
            "CREATE TRIGGER test_fail_trg BEFORE INSERT OR UPDATE "
            "ON traffic_minute_counts FOR EACH ROW EXECUTE FUNCTION test_fail_ix()"
        )
    finally:
        setup.close()
    try:
        with pytest.raises(psycopg.errors.RaiseException):
            ingest.submit_batch([rec("GOODIX", 1), rec("FAILIX", 1)])
        assert watermark(db, "GOODIX") is None
        assert rows(db, "GOODIX") == []
        assert db.execute(
            "SELECT count(*) AS n FROM traffic_ingest_observations WHERE seq = 1"
        ).fetchone()["n"] == 0
    finally:
        teardown = psycopg.connect(DSN, autocommit=True)
        try:
            teardown.execute(
                "DROP TRIGGER IF EXISTS test_fail_trg ON traffic_minute_counts"
            )
            teardown.execute("DROP FUNCTION IF EXISTS test_fail_ix()")
        finally:
            teardown.close()


def test_intersection_lock_is_exclusive_and_per_intersection(db):
    """The advisory lock used for serialization is exclusive per intersection.

    Uses ``pg_try_advisory_xact_lock`` so nothing ever blocks: this sandbox
    cannot deliver the signals PostgreSQL uses to wake lock waiters.
    """
    holder = psycopg.connect(DSN, autocommit=False)
    try:
        holder.execute("SELECT pg_advisory_xact_lock(hashtextextended('A', 0))")
        probe = psycopg.connect(DSN, autocommit=True)
        try:
            same = probe.execute(
                "SELECT pg_try_advisory_xact_lock(hashtextextended('A', 0))"
            ).fetchone()[0]
            other = probe.execute(
                "SELECT pg_try_advisory_xact_lock(hashtextextended('B', 0))"
            ).fetchone()[0]
        finally:
            probe.close()
        assert same is False, "another session must not get A's lock while held"
        assert other is True, "a different intersection must not be blocked"
    finally:
        holder.rollback()
        holder.close()


def test_different_intersections_have_different_lock_keys():
    with psycopg.connect(DSN, autocommit=True) as conn:
        same = conn.execute(
            "SELECT hashtextextended('A', 0) = hashtextextended('B', 0) AS same"
        ).fetchone()[0]
    assert same is False


def test_other_intersection_not_blocked_by_pending_gap(db):
    ingest.submit_batch([rec("A", 1), rec("A", 3)])  # A has a gap
    result = ingest.submit_batch([rec("B", 1), rec("B", 2)])
    assert result["intersections"][0]["intersection_id"] == "B"
    assert result["intersections"][0]["published_seq"] == 2
    assert watermark(db, "A") == 1


def test_observations_listing_reports_status(db):
    ingest.submit_batch([rec("A", 1), rec("A", 3)])
    published = ingest.list_observations("A", "published", 100, 0)
    pending = ingest.list_observations("A", "pending", 100, 0)
    assert [item["seq"] for item in published["items"]] == [1]
    assert [item["seq"] for item in pending["items"]] == [3]
    assert pending["items"][0]["status"] == "pending"


def test_large_backlog_publishes_across_chunk_boundaries(db, monkeypatch):
    """A gap fill spanning several publish chunks still lands in order."""
    monkeypatch.setattr(ingest, "_PUBLISH_CHUNK", 7)
    ingest.submit_batch([rec("A", 1)])
    ingest.submit_batch([rec("A", seq) for seq in range(3, 41)])
    assert watermark(db, "A") == 1
    result = ingest.submit_batch([rec("A", 2)])
    summary = result["intersections"][0]
    assert summary["published_seq"] == 40
    assert summary["pending_count"] == 0
    assert summary["published_now_seqs"] == list(range(2, 41))
    assert [r[0] for r in rows(db, "A")] == [
        BASE_MINUTE + timedelta(minutes=n) for n in range(1, 41)
    ]
