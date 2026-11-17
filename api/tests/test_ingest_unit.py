"""Unit tests for pure helpers (no database needed).

Run from the api/ directory:
    python -m pytest tests -q
Integration tests against a real PostgreSQL are skipped unless
TRAFFIC_TEST_DSN is set; see tests/test_ingest_integration.py.
"""

from datetime import datetime, timezone

import pytest

from app import ingest


def rec(ix="IX-A", seq=1, minute="2026-11-17T10:00:00Z", count=10):
    return ingest.IncomingRecord(
        intersection_id=ix,
        seq=seq,
        minute_utc=datetime.fromisoformat(minute.replace("Z", "+00:00")),
        vehicle_count=count,
    )


def test_normalize_accepts_whole_utc_minute():
    normalized = ingest._normalize(rec())
    assert normalized.minute_utc == datetime(2026, 11, 17, 10, 0, tzinfo=timezone.utc)


def test_normalize_treats_naive_timestamp_as_utc():
    naive = ingest.IncomingRecord(
        intersection_id="IX-A",
        seq=1,
        minute_utc=datetime(2026, 11, 17, 10, 0),
        vehicle_count=1,
    )
    assert ingest._normalize(naive).minute_utc.tzinfo == timezone.utc


def test_normalize_converts_other_offsets_to_utc():
    local = ingest.IncomingRecord(
        intersection_id="IX-A",
        seq=1,
        minute_utc=datetime.fromisoformat("2026-11-17T18:00:00+08:00"),
        vehicle_count=1,
    )
    assert ingest._normalize(local).minute_utc == datetime(
        2026, 11, 17, 10, 0, tzinfo=timezone.utc
    )


@pytest.mark.parametrize(
    "minute",
    ["2026-11-17T10:00:30Z", "2026-11-17T10:00:00.500Z"],
)
def test_normalize_rejects_sub_minute_timestamps(minute):
    with pytest.raises(ValueError, match="whole minute"):
        ingest._normalize(rec(minute=minute))


def test_normalize_rejects_zero_or_negative_seq():
    with pytest.raises(ValueError, match="seq"):
        ingest._normalize(rec(seq=0))


def test_normalize_rejects_negative_count():
    with pytest.raises(ValueError, match="vehicle_count"):
        ingest._normalize(rec(count=-1))


def test_iso_render_is_utc_z():
    assert ingest._iso(datetime(2026, 11, 17, 10, 0, tzinfo=timezone.utc)) == (
        "2026-11-17T10:00:00Z"
    )


def test_conflict_shape_includes_existing_and_submitted():
    item = ingest._conflict(
        ("IX-A", 7),
        "content_mismatch_with_stored_observation",
        existing=ingest._record_view(
            datetime(2026, 11, 17, 10, 0, tzinfo=timezone.utc), 10
        ),
        submitted=ingest._record_view(
            datetime(2026, 11, 17, 10, 0, tzinfo=timezone.utc), 11
        ),
    )
    assert item["intersection_id"] == "IX-A"
    assert item["seq"] == 7
    assert item["existing"]["vehicle_count"] == 10
    assert item["submitted"]["vehicle_count"] == 11
