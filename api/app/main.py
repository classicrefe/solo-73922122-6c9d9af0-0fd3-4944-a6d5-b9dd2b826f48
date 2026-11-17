"""Traffic data-warehouse PITR recovery API.

Scenario: an operator accidentally deleted intersection minute counts from
the source database. The primary keeps collecting; this service rebuilds an
isolated read-only replica from the startup physical base backup + archived
WAL, up to a user-created named restore point, so the deleted rows can be
queried and back-filled.

Demo entry point: run the stack, then open /docs (Swagger UI).
"""

from __future__ import annotations

import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import RedirectResponse
import psycopg
from psycopg.rows import dict_row
from pydantic import BaseModel, Field

from . import backup, config, db, ingest, lsn, recovery, state

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
log = logging.getLogger("api")

NAME_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$"


def _readiness_loop():
    while True:
        try:
            recovery.refresh_restore_points()
        except Exception:
            log.exception("readiness refresh failed")
        time.sleep(2)


def _monitor_loop():
    while True:
        try:
            recovery.monitor_current()
        except Exception:
            log.exception("replica monitor failed")
        time.sleep(3)


def _ensure_ingest_schema_loop():
    """Create the ingest ledger once the primary is reachable.

    The tables also exist in db/init, but a data volume created by an older
    version of the stack is upgraded in place here.
    """
    while True:
        try:
            ingest.ensure_schema()
            return
        except Exception:
            log.warning("ingest ledger schema not ready yet, retrying")
        time.sleep(2)


@asynccontextmanager
async def lifespan(_app):
    os.makedirs(config.RECOVERY_ROOT, exist_ok=True)
    os.makedirs(os.path.dirname(config.STATE_DB), exist_ok=True)
    state.init()
    recovery.cleanup_on_startup()
    # physical base backup is established at startup, in the background
    threading.Thread(target=backup.run_base_backup, daemon=True, name="base-backup").start()
    threading.Thread(target=_readiness_loop, daemon=True, name="readiness").start()
    threading.Thread(target=_monitor_loop, daemon=True, name="monitor").start()
    threading.Thread(
        target=_ensure_ingest_schema_loop, daemon=True, name="ingest-schema"
    ).start()
    yield


app = FastAPI(
    title="Traffic PITR Recovery API",
    version="1.1.0",
    description=(
        "Ordered batch ingestion for out-of-order collector retransmissions, "
        "plus physical point-in-time recovery for the traffic_minute_counts "
        "table. Ingest flow: POST /ingest/batches with records "
        "{intersection_id, seq, minute_utc, vehicle_count}; seq starts at 1. "
        "Records beyond a gap stay pending (GET /ingest/pending/{id}); once "
        "the gap is filled the contiguous run is published in sequence order, "
        "so a higher seq for the same minute overrides the earlier value. "
        "GET /ingest/watermarks gives the published watermark and "
        "next_missing_seq. Recovery flow: 1) POST /demo/generate to simulate "
        "collection, 2) POST /restore-points to create a named restore point, "
        "3) POST /demo/delete to simulate the accidental deletion, "
        "4) POST /recoveries to rebuild an isolated read-only replica at the "
        "restore point, 5) GET /replica/traffic to fetch the lost rows. "
        "The primary keeps collecting the whole time."
    ),
    lifespan=lifespan,
)


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse("/docs")


# --------------------------------------------------------------------------
# system
# --------------------------------------------------------------------------

@app.get("/health", tags=["system"])
def health():
    try:
        with db.primary() as conn:
            conn.execute("SELECT 1")
        primary_ok = True
    except Exception:
        primary_ok = False
    return {"status": "ok" if primary_ok else "degraded", "primary_reachable": primary_ok}


@app.get("/backup", tags=["system"])
def backup_status():
    """Status of the physical base backup taken at startup."""
    return {
        "status": state.kv_get("backup_status", "unknown"),
        "end_lsn": state.kv_get("backup_end_lsn"),
        "completed_at": state.kv_get("backup_completed_at"),
        "error": state.kv_get("backup_error") or None,
        "backup_dir": config.BACKUP_DIR,
    }


# --------------------------------------------------------------------------
# batch ingestion (out-of-order collector retransmissions)
# --------------------------------------------------------------------------

class IngestRecord(BaseModel):
    intersection_id: str = Field(..., min_length=1, max_length=128)
    seq: int = Field(
        ..., ge=1, le=9_223_372_036_854_775_807, description="1-based sequence number"
    )
    minute_utc: datetime = Field(..., description="UTC minute (whole minute)")
    vehicle_count: int = Field(..., ge=0, le=2_147_483_647)

    model_config = {
        "json_schema_extra": {
            "example": {
                "intersection_id": "IX-001",
                "seq": 1,
                "minute_utc": "2026-11-17T08:00:00Z",
                "vehicle_count": 12,
            }
        }
    }


class IngestBatch(BaseModel):
    records: list[IngestRecord] = Field(..., min_length=1, max_length=10000)

    model_config = {
        "json_schema_extra": {
            "example": {
                "records": [
                    {
                        "intersection_id": "IX-001",
                        "seq": 1,
                        "minute_utc": "2026-11-17T08:00:00Z",
                        "vehicle_count": 12,
                    },
                    {
                        "intersection_id": "IX-001",
                        "seq": 3,
                        "minute_utc": "2026-11-17T08:02:00Z",
                        "vehicle_count": 21,
                    },
                ]
            }
        }
    }


def _ingest_ready():
    try:
        ingest.ensure_schema()
    except Exception as exc:  # noqa: BLE001 - surfaced as 503
        raise HTTPException(503, f"ingest ledger is not available: {exc}") from exc


@app.post("/ingest/batches", tags=["ingest"])
def ingest_batch(batch: IngestBatch):
    """Accept one collector retransmission batch (idempotent, atomic).

    * records above a gap are persisted and stay ``pending``;
    * filling a gap publishes the contiguous run in ascending ``seq`` order,
      so for the same minute the higher seq overrides the earlier value;
    * re-sending the same ``(intersection_id, seq)`` with the same content is
      a no-op (``already_stored``); different content rejects the whole batch
      with 409 and nothing is written;
    * observation storage, primary-table writes and the watermark advance
      share one transaction.
    """
    _ingest_ready()
    try:
        return ingest.submit_batch(
            [
                ingest.IncomingRecord(
                    intersection_id=record.intersection_id,
                    seq=record.seq,
                    minute_utc=record.minute_utc,
                    vehicle_count=record.vehicle_count,
                )
                for record in batch.records
            ]
        )
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    except ingest.ContentConflict as exc:
        raise HTTPException(
            409,
            {
                "error": "content conflict",
                "message": (
                    "one or more sequence numbers were already stored with "
                    "different content; the whole batch was rejected"
                ),
                "conflicts": exc.conflicts,
            },
        ) from exc
    except psycopg.errors.LockNotAvailable as exc:
        # Another batch for the same intersection holds its lock.  Nothing
        # was written; the collector should retry the same batch.
        raise HTTPException(
            503,
            "another batch for the same intersection is still being applied; "
            "nothing was written, retry the batch",
        ) from exc
    except Exception as exc:  # noqa: BLE001 - storage failure, nothing committed
        log.exception("ingest batch failed")
        raise HTTPException(503, f"ingest batch failed: {exc}") from exc


@app.get("/ingest/watermarks", tags=["ingest"])
def ingest_watermarks():
    """Published watermark and next missing seq per intersection."""
    _ingest_ready()
    return {"items": ingest.list_watermarks()}


@app.get("/ingest/watermarks/{intersection_id}", tags=["ingest"])
def ingest_watermark(intersection_id: str):
    """One intersection: published watermark and next missing seq."""
    _ingest_ready()
    result = ingest.get_watermark(intersection_id)
    if result is None:
        raise HTTPException(404, f"no ingestion seen for {intersection_id!r}")
    return result


@app.get("/ingest/pending/{intersection_id}", tags=["ingest"])
def ingest_pending(
    intersection_id: str,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Persisted-but-not-yet-publishable observations for one intersection."""
    _ingest_ready()
    result = ingest.list_pending(intersection_id, limit, offset)
    if result is None:
        raise HTTPException(404, f"no ingestion seen for {intersection_id!r}")
    return result


@app.get("/ingest/observations/{intersection_id}", tags=["ingest"])
def ingest_observations(
    intersection_id: str,
    status: str = Query("all", pattern="^(all|pending|published)$"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Stored observations with their published/pending status."""
    _ingest_ready()
    result = ingest.list_observations(intersection_id, status, limit, offset)
    if result is None:
        raise HTTPException(404, f"no ingestion seen for {intersection_id!r}")
    return result


@app.get("/ingest/traffic/{intersection_id}", tags=["ingest"])
def ingest_traffic(intersection_id: str):
    """Current primary-table rows for one intersection (published data)."""
    _ingest_ready()
    with db.primary(row_factory=dict_row) as conn:
        conn.execute("SET TIME ZONE 'UTC'")
        rows = conn.execute(
            f"SELECT intersection_id, minute_utc, vehicle_count "
            f"FROM {config.TRAFFIC_TABLE} WHERE intersection_id = %s "
            f"ORDER BY minute_utc",
            (intersection_id,),
        ).fetchall()
    return {
        "intersection_id": intersection_id,
        "rows": [
            {
                "intersection_id": row["intersection_id"],
                "minute_utc": row["minute_utc"].isoformat().replace("+00:00", "Z"),
                "vehicle_count": row["vehicle_count"],
            }
            for row in rows
        ],
    }


# --------------------------------------------------------------------------
# restore points
# --------------------------------------------------------------------------

@app.post("/restore-points", status_code=201, tags=["restore-points"])
def create_restore_point(name: str = Query(..., pattern=NAME_PATTERN)):
    """Create a named restore point on the primary (pg_create_restore_point).

    The point becomes selectable for recovery only after every WAL segment
    between the base backup and the point has shown up in the archive.
    """
    if state.kv_get("backup_status") != "ready":
        raise HTTPException(
            409, "base backup not ready yet; restore points must be created after it"
        )
    if state.one("SELECT id FROM restore_points WHERE name=?", (name,)):
        raise HTTPException(409, f"restore point {name!r} already exists")
    with db.primary() as conn:
        point_lsn = conn.execute(
            "SELECT pg_create_restore_point(%s)::text", (name,)
        ).fetchone()[0]
        # force the segment holding the point to be archived promptly
        conn.execute("SELECT pg_switch_wal()")
    wal_file = lsn.wal_file(lsn.parse(point_lsn))
    point_id = state.exec(
        "INSERT INTO restore_points (name, lsn, wal_file) VALUES (?, ?, ?)",
        (name, point_lsn, wal_file),
    )
    recovery.refresh_restore_points()
    return state.one("SELECT * FROM restore_points WHERE id=?", (point_id,))


@app.get("/restore-points", tags=["restore-points"])
def list_restore_points():
    recovery.refresh_restore_points()
    return {"items": state.q("SELECT * FROM restore_points ORDER BY id")}


# --------------------------------------------------------------------------
# recoveries
# --------------------------------------------------------------------------

class RecoveryRequest(BaseModel):
    restore_point: str = Field(..., pattern=NAME_PATTERN)


@app.post("/recoveries", status_code=202, tags=["recoveries"])
def start_recovery(req: RecoveryRequest):
    """Start a recovery to a named restore point (only one may be in flight).

    Rebuilds a fresh isolated replica from the physical base backup; any
    previously served replica is retired first, so points are never mixed.
    """
    try:
        return recovery.start_recovery(req.restore_point)
    except recovery.NotFound as exc:
        raise HTTPException(404, str(exc)) from exc
    except recovery.Conflict as exc:
        raise HTTPException(409, str(exc)) from exc


@app.get("/recoveries", tags=["recoveries"])
def list_recoveries():
    return {"items": state.q("SELECT * FROM recoveries ORDER BY id DESC")}


@app.get("/recoveries/current", tags=["recoveries"])
def current_recovery():
    try:
        return recovery.require_current()
    except recovery.Conflict as exc:
        raise HTTPException(404, str(exc)) from exc


@app.get("/recoveries/{rec_id}", tags=["recoveries"])
def get_recovery(rec_id: int):
    rec = state.one("SELECT * FROM recoveries WHERE id=?", (rec_id,))
    if not rec:
        raise HTTPException(404, f"recovery {rec_id} not found")
    return rec


# --------------------------------------------------------------------------
# querying the recovered replica
# --------------------------------------------------------------------------

@app.get("/replica/traffic", tags=["replica"])
def replica_traffic(
    intersection_id: str | None = None,
    limit: int = Query(100, ge=1, le=1000),
):
    """Query the recovered read-only replica (409 while none is servable)."""
    try:
        rec = recovery.require_current()
    except recovery.Conflict as exc:
        raise HTTPException(409, str(exc)) from exc
    try:
        with db.replica() as conn:
            where = "WHERE intersection_id = %(id)s" if intersection_id else ""
            rows = conn.execute(
                f"SELECT intersection_id, minute_utc, vehicle_count "
                f"FROM {config.TRAFFIC_TABLE} {where} "
                f"ORDER BY minute_utc DESC, intersection_id LIMIT %(lim)s",
                {"id": intersection_id, "lim": limit},
            ).fetchall()
            total = conn.execute(
                f"SELECT count(*) AS n FROM {config.TRAFFIC_TABLE}"
            ).fetchone()["n"]
            meta = conn.execute(
                "SELECT pg_is_in_recovery() AS in_recovery, "
                "pg_last_wal_replay_lsn()::text AS replay_lsn"
            ).fetchone()
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(503, f"replica query failed: {exc}") from exc
    return {
        "recovery": {
            "id": rec["id"],
            "restore_point": rec["restore_point"],
            "target_lsn": rec["target_lsn"],
            "replay_lsn": meta["replay_lsn"],
            "in_recovery_read_only": meta["in_recovery"],
        },
        "total_rows": total,
        "rows": rows,
    }


# --------------------------------------------------------------------------
# demo: simulated collection + accidental deletion on the primary
# --------------------------------------------------------------------------

@app.post("/demo/generate", tags=["demo"])
def demo_generate(
    intersections: int = Query(3, ge=1, le=100),
    minutes: int = Query(20, ge=1, le=10000),
    prefix: str = Query("IX", pattern=r"^[A-Za-z0-9_-]{1,16}$"),
):
    """Simulate collection: upsert intersection/minute counts on the primary."""
    sql = f"""
        INSERT INTO {config.TRAFFIC_TABLE} (intersection_id, minute_utc, vehicle_count)
        SELECT %(prefix)s || '-' || i,
               date_trunc('minute', now()) - (m || ' minutes')::interval,
               (random() * 90 + 10)::int
        FROM generate_series(1, %(n)s) AS i
        CROSS JOIN generate_series(1, %(m)s) AS m
        ON CONFLICT (intersection_id, minute_utc)
        DO UPDATE SET vehicle_count = EXCLUDED.vehicle_count
        RETURNING intersection_id
    """
    with db.primary() as conn:
        inserted = len(
            conn.execute(
                sql, {"prefix": prefix, "n": intersections, "m": minutes}
            ).fetchall()
        )
    return {
        "upserted_rows": inserted,
        "intersections": [f"{prefix}-{i}" for i in range(1, intersections + 1)],
        "minutes_covered": minutes,
    }


@app.post("/demo/delete", tags=["demo"])
def demo_delete(minutes: int = Query(10, ge=1, le=100000)):
    """Simulate the accident: delete the latest `minutes` minutes of data."""
    sql = f"""
        DELETE FROM {config.TRAFFIC_TABLE}
        WHERE minute_utc > COALESCE(
                  (SELECT max(minute_utc) FROM {config.TRAFFIC_TABLE}),
                  date_trunc('minute', now())
              ) - (%(m)s || ' minutes')::interval
        RETURNING intersection_id
    """
    with db.primary() as conn:
        deleted = len(conn.execute(sql, {"m": minutes}).fetchall())
    return {"deleted_rows": deleted, "note": "simulated accidental deletion on the primary"}


@app.get("/primary/traffic", tags=["demo"])
def primary_traffic(
    intersection_id: str | None = None,
    limit: int = Query(100, ge=1, le=1000),
):
    """Current state of the primary (which keeps collecting during recovery)."""
    where = "WHERE intersection_id = %(id)s" if intersection_id else ""
    with db.primary(row_factory=dict_row) as conn:
        rows = conn.execute(
            f"SELECT intersection_id, minute_utc, vehicle_count "
            f"FROM {config.TRAFFIC_TABLE} {where} "
            f"ORDER BY minute_utc DESC, intersection_id LIMIT %(lim)s",
            {"id": intersection_id, "lim": limit},
        ).fetchall()
        total = conn.execute(
            f"SELECT count(*) AS n FROM {config.TRAFFIC_TABLE}"
        ).fetchone()["n"]
    return {"total_rows": total, "rows": rows}


@app.get("/demo/overview", tags=["demo"])
def demo_overview():
    """One-shot picture: primary data, backup, restore points, recoveries."""
    with db.primary(row_factory=dict_row) as conn:
        total = conn.execute(
            f"SELECT count(*) AS n FROM {config.TRAFFIC_TABLE}"
        ).fetchone()["n"]
        per_ix = conn.execute(
            f"SELECT intersection_id, count(*) AS rows, "
            f"min(minute_utc) AS first_minute, max(minute_utc) AS last_minute "
            f"FROM {config.TRAFFIC_TABLE} GROUP BY 1 ORDER BY 1"
        ).fetchall()
    return {
        "primary": {"total_rows": total, "per_intersection": per_ix},
        "backup": backup_status(),
        "restore_points": state.q("SELECT * FROM restore_points ORDER BY id"),
        "recoveries": state.q("SELECT * FROM recoveries ORDER BY id DESC"),
        "current_recovery_id": state.kv_get("current_recovery_id"),
    }
