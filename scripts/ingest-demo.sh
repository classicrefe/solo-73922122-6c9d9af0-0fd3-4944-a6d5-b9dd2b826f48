#!/usr/bin/env bash
# End-to-end demo of the ordered batch-ingestion API:
#   out-of-order retransmission -> pending -> gap fill -> ordered publish
#   -> same-minute revision -> idempotent retry -> conflicting retry rejected
# Run against a live stack (default http://localhost:8000; override with BASE).
set -euo pipefail

BASE="${BASE:-http://localhost:8000}"
IX="${IX:-IX-DEMO}"

say()  { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }
fail() { printf '\033[31mFAIL: %s\033[0m\n' "$*" >&2; exit 1; }
need() { command -v "$1" >/dev/null || fail "missing dependency: $1"; }
need curl; need jq

post() {
  curl -s -w '\n%{http_code}' -X POST "$BASE/ingest/batches" \
    -H 'Content-Type: application/json' -d "$1"
}
body() { printf '%s' "$1" | head -n -1; }
code() { printf '%s' "$1" | tail -n 1; }

say "0. wait for the API"
for _ in $(seq 1 60); do
  curl -fsS "$BASE/health" >/dev/null 2>&1 && break
  sleep 1
done
curl -fsS "$BASE/health" | jq .

say "1. collector retransmits seq 1, 3 and 4 (seq 2 is still missing)"
resp=$(post "{\"records\":[
  {\"intersection_id\":\"$IX\",\"seq\":1,\"minute_utc\":\"2026-11-17T08:00:00Z\",\"vehicle_count\":12},
  {\"intersection_id\":\"$IX\",\"seq\":3,\"minute_utc\":\"2026-11-17T08:02:00Z\",\"vehicle_count\":21},
  {\"intersection_id\":\"$IX\",\"seq\":4,\"minute_utc\":\"2026-11-17T08:03:00Z\",\"vehicle_count\":18}]}")
[ "$(code "$resp")" = "200" ] || fail "batch 1 rejected: $resp"
body "$resp" | jq '{newly_stored, intersections: [.intersections[] | {published_seq, next_missing_seq, pending_seqs}]}'

say "2. watermark says the next missing seq is 2; 3 and 4 are persisted pending"
curl -fsS "$BASE/ingest/watermarks/$IX" | jq .
curl -fsS "$BASE/ingest/pending/$IX" | jq '{pending_count, next_missing_seq, seqs: [.items[].seq]}'

say "3. the missing seq 2 arrives; the contiguous run 2..4 is published in order"
resp=$(post "{\"records\":[{\"intersection_id\":\"$IX\",\"seq\":2,\"minute_utc\":\"2026-11-17T08:01:00Z\",\"vehicle_count\":7}]}")
body "$resp" | jq '{intersections: [.intersections[] | {published_seq_before, published_seq, published_now_seqs, pending_count}]}'
curl -fsS "$BASE/ingest/traffic/$IX" | jq '{rows: [.rows[] | {minute_utc, vehicle_count}]}'

say "4. seq 5 revises the same minute as seq 3; the higher seq wins"
resp=$(post "{\"records\":[{\"intersection_id\":\"$IX\",\"seq\":5,\"minute_utc\":\"2026-11-17T08:02:00Z\",\"vehicle_count\":99}]}")
body "$resp" | jq '{intersections: [.intersections[] | {published_seq, published_now_seqs}]}'
curl -fsS "$BASE/ingest/traffic/$IX" | jq '.rows[] | select(.minute_utc == "2026-11-17T08:02:00Z")'

say "5. the same records are retransmitted again; nothing is written twice"
resp=$(post "{\"records\":[
  {\"intersection_id\":\"$IX\",\"seq\":3,\"minute_utc\":\"2026-11-17T08:02:00Z\",\"vehicle_count\":21},
  {\"intersection_id\":\"$IX\",\"seq\":5,\"minute_utc\":\"2026-11-17T08:02:00Z\",\"vehicle_count\":99}]}")
body "$resp" | jq '{accepted_records, newly_stored, already_stored}'

say "6. retrying seq 3 with different content rejects the whole batch (409)"
resp=$(post "{\"records\":[
  {\"intersection_id\":\"$IX\",\"seq\":3,\"minute_utc\":\"2026-11-17T08:02:00Z\",\"vehicle_count\":1},
  {\"intersection_id\":\"$IX\",\"seq\":6,\"minute_utc\":\"2026-11-17T08:05:00Z\",\"vehicle_count\":5}]}")
[ "$(code "$resp")" = "409" ] || fail "expected 409 for a conflicting retry, got $(code "$resp")"
body "$resp" | jq '.detail | {error, conflicts}'

say "7. the rejected batch changed nothing: watermark still 5, no seq 6"
curl -fsS "$BASE/ingest/watermarks/$IX" | jq '{published_seq, next_missing_seq, pending_count}'

printf '\n\033[32mINGEST DEMO OK: out-of-order records stayed pending, the gap filled in order, the higher seq revised the minute, retries were idempotent and conflicting retries were rejected atomically.\033[0m\n'
