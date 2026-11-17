"""Ordered batch ingestion for out-of-order collector retransmissions.

Collectors retransmit minute counts after a network outage.  Every observation
is identified by ``(intersection_id, seq)`` where ``seq`` starts at 1 and
increases by one.  Observations are allowed to arrive out of order:

* every accepted observation is persisted in ``traffic_ingest_observations``;
* the per-intersection watermark is the highest contiguous sequence already
  published to the existing ``traffic_minute_counts`` table;
* observations above the watermark stay pending until all gaps below them are
  filled;
* once a gap is filled, the whole contiguous run is applied in ascending
  sequence order.  For the same minute, the record with the highest sequence
  therefore overwrites earlier revisions;
* retrying the same ``(intersection_id, seq)`` with the same content is
  idempotent; a different content for an already stored sequence rejects the
  entire batch.

The observation inserts, primary-table writes and watermark advance all run in
one PostgreSQL transaction, so they succeed or fail together.  Locks are taken
per intersection (in sorted order), so different intersections keep collecting
concurrently and API restarts cannot lose pending observations.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timezone

from psycopg.rows import dict_row

from . import config, db

log = logging.getLogger("ingest")

# How many pending observations are read/published per statement while filling
# a contiguous run; keeps memory bounded for a long outage backlog.
_PUBLISH_CHUNK = 1000

# How many pending sequence numbers are echoed in a batch response summary.
_SUMMARY_PENDING_SEQS = 1000

# Kept in sync with db/init/01-init.sql.  Executed at API startup too, so an
# existing data volume from an older deployment is upgraded in place.
SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS traffic_ingest_observations (
        intersection_id text        NOT NULL,
        seq             bigint      NOT NULL CHECK (seq >= 1),
        minute_utc      timestamptz NOT NULL,
        vehicle_count   integer     NOT NULL CHECK (vehicle_count >= 0),
        received_at     timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (intersection_id, seq)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS traffic_ingest_watermarks (
        intersection_id text        PRIMARY KEY,
        published_seq   bigint      NOT NULL DEFAULT 0 CHECK (published_seq >= 0),
        updated_at      timestamptz NOT NULL DEFAULT now()
    )
    """,
)

_schema_lock = threading.Lock()
_schema_ready = False


def ensure_schema():
    """Create the ingest ledger tables if they do not exist yet."""
    global _schema_ready
    if _schema_ready:
        return
    with _schema_lock:
        if _schema_ready:
            return
        with db.primary() as conn:
            for statement in SCHEMA_STATEMENTS:
                conn.execute(statement)
        _schema_ready = True
        log.info("ingest ledger schema is ready")


@dataclass(frozen=True)
class IncomingRecord:
    intersection_id: str
    seq: int
    minute_utc: datetime
    vehicle_count: int


class ContentConflict(Exception):
    """Raised when a sequence number is reused with different content."""

    def __init__(self, conflicts: list[dict]):
        super().__init__("content conflict")
        self.conflicts = conflicts


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------

def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _as_utc(value: datetime) -> datetime:
    # A timestamp without an offset is interpreted as UTC: the API contract
    # says the minute is UTC and this avoids silently using the server zone.
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _normalize(record: IncomingRecord) -> IncomingRecord:
    minute = _as_utc(record.minute_utc)
    if minute.second != 0 or minute.microsecond != 0:
        raise ValueError("minute_utc must be aligned to a whole minute")
    if record.seq < 1:
        raise ValueError("seq must be >= 1")
    if record.vehicle_count < 0:
        raise ValueError("vehicle_count must be >= 0")
    return IncomingRecord(
        intersection_id=record.intersection_id,
        seq=record.seq,
        minute_utc=minute,
        vehicle_count=record.vehicle_count,
    )


def _record_view(minute: datetime, count: int) -> dict:
    return {"minute_utc": _iso(minute), "vehicle_count": count}


def _conflict(key, reason, existing=None, submitted=None) -> dict:
    item = {
        "intersection_id": key[0],
        "seq": key[1],
        "reason": reason,
    }
    if existing is not None:
        item["existing"] = existing
    if submitted is not None:
        item["submitted"] = submitted
    return item


def _watermark_row(conn, intersection_id: str):
    return conn.execute(
        "SELECT intersection_id, published_seq, updated_at "
        "FROM traffic_ingest_watermarks WHERE intersection_id = %s",
        (intersection_id,),
    ).fetchone()


# --------------------------------------------------------------------------
# batch ingestion
# --------------------------------------------------------------------------

def submit_batch(records: list[IncomingRecord]) -> dict:
    """Validate and atomically persist one collector retransmission batch.

    All records in the request belong to one transaction.  A content conflict
    anywhere rejects the whole batch (no observation, no primary-table write
    and no watermark change is committed).  Other intersections use separate
    watermark rows and are not blocked.
    """
    normalized = [_normalize(record) for record in records]
    for record in normalized:
        if not record.intersection_id.strip():
            raise ValueError("intersection_id must not be blank")

    # Collapse duplicate keys inside the request.  Identical repeats are
    # idempotent; a differing repeat is a batch-level conflict.
    unique: dict[tuple[str, int], IncomingRecord] = {}
    duplicates_in_batch = 0
    for record in normalized:
        key = (record.intersection_id, record.seq)
        previous = unique.get(key)
        if previous is None:
            unique[key] = record
        elif (
            previous.minute_utc == record.minute_utc
            and previous.vehicle_count == record.vehicle_count
        ):
            duplicates_in_batch += 1
        else:
            raise ContentConflict(
                [
                    _conflict(
                        key,
                        "content_mismatch_within_batch",
                        existing=_record_view(
                            previous.minute_utc, previous.vehicle_count
                        ),
                        submitted=_record_view(
                            record.minute_utc, record.vehicle_count
                        ),
                    )
                ]
            )

    groups: dict[str, list[IncomingRecord]] = {}
    for record in unique.values():
        groups.setdefault(record.intersection_id, []).append(record)

    intersections = sorted(groups)

    with db.primary_transaction(row_factory=dict_row) as conn:
        conn.execute("SET TIME ZONE 'UTC'")
        conn.execute(
            "SELECT set_config('lock_timeout', %s, true)",
            (f"{config.INGEST_LOCK_TIMEOUT_MS}ms",),
        )

        # Serialize every batch touching the same intersection with a
        # transaction-scoped advisory lock, taken in sorted order (so batches
        # covering several intersections can never deadlock).  Advisory locks
        # avoid relying on row locks whose interaction with
        # INSERT ... ON CONFLICT can block concurrent batches for a brand-new
        # intersection.  Other intersections have different keys and are never
        # blocked by one slow or retrying collector.
        watermarks: dict[str, int] = {}
        for intersection_id in intersections:
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (intersection_id,),
            )
            conn.execute(
                "INSERT INTO traffic_ingest_watermarks (intersection_id) "
                "VALUES (%s) ON CONFLICT (intersection_id) DO NOTHING",
                (intersection_id,),
            )
            row = conn.execute(
                "SELECT published_seq FROM traffic_ingest_watermarks "
                "WHERE intersection_id = %s",
                (intersection_id,),
            ).fetchone()
            if row is None:
                raise RuntimeError(f"watermark row for {intersection_id!r} is missing")
            watermarks[intersection_id] = int(row["published_seq"])

        # Load observations that already exist for the submitted keys.
        existing: dict[tuple[str, int], dict] = {}
        for intersection_id, records_for_ix in groups.items():
            seqs = sorted({record.seq for record in records_for_ix})
            rows = conn.execute(
                "SELECT seq, minute_utc, vehicle_count "
                "FROM traffic_ingest_observations "
                "WHERE intersection_id = %s AND seq = ANY(%s)",
                (intersection_id, seqs),
            ).fetchall()
            for row in rows:
                existing[(intersection_id, int(row["seq"]))] = row

        conflicts: list[dict] = []
        new_records: list[IncomingRecord] = []
        for key, record in unique.items():
            old = existing.get(key)
            if old is None:
                if record.seq <= watermarks[record.intersection_id]:
                    # All sequences at/below the watermark have been published,
                    # so their observation must exist.  Refuse to guess if the
                    # ledger was tampered with.
                    conflicts.append(
                        _conflict(
                            key,
                            "missing_observation_below_watermark",
                            submitted=_record_view(
                                record.minute_utc, record.vehicle_count
                            ),
                        )
                    )
                else:
                    new_records.append(record)
            else:
                same_content = (
                    old["minute_utc"].astimezone(timezone.utc)
                    == record.minute_utc
                    and int(old["vehicle_count"]) == record.vehicle_count
                )
                if not same_content:
                    conflicts.append(
                        _conflict(
                            key,
                            "content_mismatch_with_stored_observation",
                            existing=_record_view(
                                old["minute_utc"], int(old["vehicle_count"])
                            ),
                            submitted=_record_view(
                                record.minute_utc, record.vehicle_count
                            ),
                        )
                    )

        if conflicts:
            # Raises inside the transaction context -> full rollback.
            raise ContentConflict(conflicts)

        if new_records:
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO traffic_ingest_observations "
                    "(intersection_id, seq, minute_utc, vehicle_count) "
                    "VALUES (%s, %s, %s, %s)",
                    [
                        (
                            record.intersection_id,
                            record.seq,
                            record.minute_utc,
                            record.vehicle_count,
                        )
                        for record in new_records
                    ],
                )

        summaries: list[dict] = []
        published_now: dict[str, set[int]] = {}
        for intersection_id in intersections:
            watermark_before = watermarks[intersection_id]

            # Publish the contiguous run above the watermark, in ascending
            # sequence order, a bounded chunk at a time.  A later chunk only
            # starts at the exact seq the previous one ended with, so a gap
            # (or concurrently impossible-but-defensive duplicate) can never be
            # skipped.  For the same minute a later seq overwrites the earlier
            # revision in traffic_minute_counts.
            watermark_after = watermark_before
            published_seqs: list[int] = []
            while True:
                chunk = conn.execute(
                    "SELECT seq, minute_utc, vehicle_count "
                    "FROM traffic_ingest_observations "
                    "WHERE intersection_id = %s AND seq > %s "
                    "ORDER BY seq LIMIT %s",
                    (intersection_id, watermark_after, _PUBLISH_CHUNK),
                ).fetchall()
                contiguous = []
                for row in chunk:
                    seq = int(row["seq"])
                    if seq != watermark_after + 1:
                        break  # gap -> everything from here stays pending
                    watermark_after = seq
                    contiguous.append(row)
                if not contiguous:
                    break
                with conn.cursor() as cur:
                    cur.executemany(
                        f"INSERT INTO {config.TRAFFIC_TABLE} "
                        "(intersection_id, minute_utc, vehicle_count) "
                        "VALUES (%s, %s, %s) "
                        "ON CONFLICT (intersection_id, minute_utc) "
                        "DO UPDATE SET vehicle_count = EXCLUDED.vehicle_count",
                        [
                            (
                                intersection_id,
                                row["minute_utc"],
                                int(row["vehicle_count"]),
                            )
                            for row in contiguous
                        ],
                    )
                published_seqs.extend(int(row["seq"]) for row in contiguous)
                if len(contiguous) < len(chunk) or len(chunk) < _PUBLISH_CHUNK:
                    break

            if watermark_after != watermark_before:
                updated = conn.execute(
                    "UPDATE traffic_ingest_watermarks "
                    "SET published_seq = %s, updated_at = now() "
                    "WHERE intersection_id = %s AND published_seq <= %s",
                    (watermark_after, intersection_id, watermark_after),
                ).rowcount
                if updated != 1:
                    raise RuntimeError(
                        "watermark advance was not applied for "
                        f"{intersection_id!r} (rowcount={updated})"
                    )
            elif new_records:
                # Only touch the row (and its updated_at) when this batch
                # actually stored something; a pure idempotent retry is a
                # read-only transaction.
                conn.execute(
                    "UPDATE traffic_ingest_watermarks SET updated_at = now() "
                    "WHERE intersection_id = %s",
                    (intersection_id,),
                )

            pending_total = conn.execute(
                "SELECT count(*) AS n FROM traffic_ingest_observations "
                "WHERE intersection_id = %s AND seq > %s",
                (intersection_id, watermark_after),
            ).fetchone()["n"]
            pending_head = conn.execute(
                "SELECT seq FROM traffic_ingest_observations "
                "WHERE intersection_id = %s AND seq > %s "
                "ORDER BY seq LIMIT %s",
                (intersection_id, watermark_after, _SUMMARY_PENDING_SEQS),
            ).fetchall()
            published_now[intersection_id] = set(published_seqs)
            stored_new = sum(
                1
                for record in new_records
                if record.intersection_id == intersection_id
            )
            accepted_for_ix = sum(
                1
                for record in normalized
                if record.intersection_id == intersection_id
            )
            summaries.append(
                {
                    "intersection_id": intersection_id,
                    "published_seq_before": watermark_before,
                    "published_seq": watermark_after,
                    "next_missing_seq": watermark_after + 1,
                    "published_now_count": len(published_seqs),
                    "published_now_seqs": published_seqs,
                    "pending_count": int(pending_total),
                    "pending_seqs": [int(row["seq"]) for row in pending_head],
                    "pending_seqs_truncated": int(pending_total) > len(pending_head),
                    "accepted_records": accepted_for_ix,
                    "newly_stored": stored_new,
                }
            )

    # Build the per-input-record view outside the transaction.  Duplicate
    # occurrences share the same key; only the first occurrence can be the one
    # that was newly stored.
    new_keys = {(record.intersection_id, record.seq) for record in new_records}
    seen: set[tuple[str, int]] = set()
    record_results = []
    for record in normalized:
        key = (record.intersection_id, record.seq)
        newly_stored = key in new_keys and key not in seen
        seen.add(key)
        summary_ix = next(
            item for item in summaries
            if item["intersection_id"] == record.intersection_id
        )
        record_results.append(
            {
                "intersection_id": record.intersection_id,
                "seq": record.seq,
                "minute_utc": _iso(record.minute_utc),
                "vehicle_count": record.vehicle_count,
                "status": (
                    "published"
                    if record.seq <= summary_ix["published_seq"]
                    else "pending"
                ),
                "newly_stored": newly_stored,
                "published_in_this_batch": (
                    record.seq in published_now[record.intersection_id]
                ),
            }
        )

    return {
        "accepted_records": len(normalized),
        "unique_records": len(unique),
        "newly_stored": len(new_records),
        "already_stored": len(normalized) - len(new_records) - duplicates_in_batch,
        "duplicates_in_batch": duplicates_in_batch,
        "content_conflicts": 0,
        "intersections": summaries,
        "records": record_results,
    }


# --------------------------------------------------------------------------
# queries
# --------------------------------------------------------------------------

def list_watermarks() -> list[dict]:
    with db.primary(row_factory=dict_row) as conn:
        conn.execute("SET TIME ZONE 'UTC'")
        rows = conn.execute(
            """
            SELECT w.intersection_id,
                   w.published_seq,
                   w.updated_at,
                   (SELECT count(*)
                      FROM traffic_ingest_observations o
                     WHERE o.intersection_id = w.intersection_id
                       AND o.seq > w.published_seq) AS pending_count,
                   (SELECT min(o.seq)
                      FROM traffic_ingest_observations o
                     WHERE o.intersection_id = w.intersection_id
                       AND o.seq > w.published_seq) AS first_pending_seq,
                   (SELECT max(o.seq)
                      FROM traffic_ingest_observations o
                     WHERE o.intersection_id = w.intersection_id) AS last_received_seq
            FROM traffic_ingest_watermarks w
            ORDER BY w.intersection_id
            """
        ).fetchall()
    return [
        {
            "intersection_id": row["intersection_id"],
            "published_seq": int(row["published_seq"]),
            "next_missing_seq": int(row["published_seq"]) + 1,
            "pending_count": int(row["pending_count"]),
            "first_pending_seq": (
                int(row["first_pending_seq"])
                if row["first_pending_seq"] is not None
                else None
            ),
            "last_received_seq": (
                int(row["last_received_seq"])
                if row["last_received_seq"] is not None
                else None
            ),
            "updated_at": _iso(row["updated_at"]),
        }
        for row in rows
    ]


def get_watermark(intersection_id: str, pending_limit: int = 50) -> dict | None:
    with db.primary(row_factory=dict_row) as conn:
        conn.execute("SET TIME ZONE 'UTC'")
        row = _watermark_row(conn, intersection_id)
        if not row:
            return None
        watermark = int(row["published_seq"])
        pending = conn.execute(
            "SELECT seq FROM traffic_ingest_observations "
            "WHERE intersection_id = %s AND seq > %s ORDER BY seq LIMIT %s",
            (intersection_id, watermark, pending_limit),
        ).fetchall()
        total = conn.execute(
            "SELECT count(*) AS n FROM traffic_ingest_observations "
            "WHERE intersection_id = %s AND seq > %s",
            (intersection_id, watermark),
        ).fetchone()["n"]
        last_received = conn.execute(
            "SELECT max(seq) AS m FROM traffic_ingest_observations "
            "WHERE intersection_id = %s",
            (intersection_id,),
        ).fetchone()["m"]
    return {
        "intersection_id": intersection_id,
        "published_seq": watermark,
        "next_missing_seq": watermark + 1,
        "pending_count": int(total),
        "first_pending_seq": int(pending[0]["seq"]) if pending else None,
        "last_received_seq": int(last_received) if last_received is not None else None,
        "pending_seqs": [int(item["seq"]) for item in pending],
        "pending_seqs_truncated": int(total) > len(pending),
        "updated_at": _iso(row["updated_at"]),
    }


def list_pending(intersection_id: str, limit: int, offset: int) -> dict | None:
    with db.primary(row_factory=dict_row) as conn:
        conn.execute("SET TIME ZONE 'UTC'")
        row = _watermark_row(conn, intersection_id)
        if not row:
            return None
        watermark = int(row["published_seq"])
        total = conn.execute(
            "SELECT count(*) AS n FROM traffic_ingest_observations "
            "WHERE intersection_id = %s AND seq > %s",
            (intersection_id, watermark),
        ).fetchone()["n"]
        items = conn.execute(
            "SELECT seq, minute_utc, vehicle_count, received_at "
            "FROM traffic_ingest_observations "
            "WHERE intersection_id = %s AND seq > %s "
            "ORDER BY seq LIMIT %s OFFSET %s",
            (intersection_id, watermark, limit, offset),
        ).fetchall()
    return {
        "intersection_id": intersection_id,
        "published_seq": watermark,
        "next_missing_seq": watermark + 1,
        "pending_count": int(total),
        "items": [
            {
                "intersection_id": intersection_id,
                "seq": int(item["seq"]),
                "minute_utc": _iso(item["minute_utc"]),
                "vehicle_count": int(item["vehicle_count"]),
                "received_at": _iso(item["received_at"]),
            }
            for item in items
        ],
    }


def list_observations(
    intersection_id: str, status: str, limit: int, offset: int
) -> dict | None:
    with db.primary(row_factory=dict_row) as conn:
        conn.execute("SET TIME ZONE 'UTC'")
        row = _watermark_row(conn, intersection_id)
        if not row:
            return None
        watermark = int(row["published_seq"])
        where = "intersection_id = %s"
        params: list = [intersection_id]
        if status == "published":
            where += " AND seq <= %s"
            params.append(watermark)
        elif status == "pending":
            where += " AND seq > %s"
            params.append(watermark)
        total = conn.execute(
            f"SELECT count(*) AS n FROM traffic_ingest_observations WHERE {where}",
            params,
        ).fetchone()["n"]
        items = conn.execute(
            f"SELECT seq, minute_utc, vehicle_count, received_at "
            f"FROM traffic_ingest_observations WHERE {where} "
            f"ORDER BY seq LIMIT %s OFFSET %s",
            (*params, limit, offset),
        ).fetchall()
    return {
        "intersection_id": intersection_id,
        "published_seq": watermark,
        "next_missing_seq": watermark + 1,
        "status_filter": status,
        "total": int(total),
        "items": [
            {
                "intersection_id": intersection_id,
                "seq": int(item["seq"]),
                "minute_utc": _iso(item["minute_utc"]),
                "vehicle_count": int(item["vehicle_count"]),
                "received_at": _iso(item["received_at"]),
                "status": (
                    "published" if int(item["seq"]) <= watermark else "pending"
                ),
            }
            for item in items
        ],
    }
