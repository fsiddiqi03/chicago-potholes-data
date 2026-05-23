"""
Load Chicago pothole records from the Socrata API into the potholes table.

Three operating modes via CLI:
  --backfill --since YYYY-MM-DD   Initial historical load (uses created_date filter)
  (no args)                       Daily incremental (uses last successful run timestamp)
  --dry-run [--limit N]           Fetch & normalize but don't write to DB

Run with:
    python -m src.loaders.load_potholes --backfill --since 2024-05-21
    python -m src.loaders.load_potholes
    python -m src.loaders.load_potholes --dry-run --limit 50
"""
from __future__ import annotations

import argparse
import logging
import sys
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

import psycopg2
from psycopg2.extras import Json, execute_batch

from ..db import get_connection
from ..socrata import fetch_potholes
from ..transforms.pothole import normalize_record
from ..transforms.stats import refresh_all

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# Tunables.
UPSERT_BATCH_SIZE = 500       # how many rows per executemany batch
PROGRESS_LOG_INTERVAL = 5000  # log progress every N records


# =============================================================
# Ingest run lifecycle (ingest_runs table)
# =============================================================
def start_ingest_run(cursor: Any, mode: str) -> str:
    """Insert a new ingest_runs row with status='running'. Returns its id."""
    run_id = str(uuid.uuid4())
    cursor.execute(
        """
        INSERT INTO ingest_runs (id, started_at, status, error_message)
        VALUES (%s, NOW(), 'running', %s)
        """,
        (run_id, f"mode={mode}"),
    )
    logger.info("Started ingest run %s (mode=%s)", run_id, mode)
    return run_id


def finish_ingest_run(
    cursor: Any,
    run_id: str,
    status: str,
    records_fetched: int,
    records_added: int,
    records_updated: int,
    error_message: Optional[str] = None,
) -> None:
    """Update the ingest_runs row with final status and counts."""
    cursor.execute(
        """
        UPDATE ingest_runs
        SET completed_at    = NOW(),
            status          = %s,
            records_fetched = %s,
            records_added   = %s,
            records_updated = %s,
            error_message   = %s
        WHERE id = %s
        """,
        (status, records_fetched, records_added, records_updated, error_message, run_id),
    )


def get_last_successful_run_start(cursor: Any) -> Optional[datetime]:
    """
    Look up when the most recent successful run *started*.
    Returns None if there has never been a successful run.

    We use started_at, not completed_at, because last_modified_date
    on Socrata records reflects when the city updated them — and we
    want to catch anything modified at or after the moment our last
    sync began, even if the sync took a while to finish.
    """
    cursor.execute(
        """
        SELECT started_at
        FROM ingest_runs
        WHERE status = 'success'
        ORDER BY started_at DESC
        LIMIT 1
        """
    )
    row = cursor.fetchone()
    return row[0] if row else None


# =============================================================
# Upsert SQL — first pass (insert/update rows, leave duplicate_of NULL)
# =============================================================
# Notes on this SQL:
# - ON CONFLICT on source_id makes the operation idempotent. Safe to re-run.
# - We compute the ward via ST_Contains and prefer the spatial lookup over
#   the city's ward field, but fall back to the city's value if spatial
#   lookup returns null. This handles both city errors and edge cases.
# - first_seen_at is set on INSERT only (via COALESCE with the existing value
#   on conflict), so we preserve the "when did WE first see this" timeline.
# - duplicate_of is NOT set here — second pass handles that.
UPSERT_POTHOLE_SQL = """
WITH input AS (
    SELECT
        %(source_id)s::text        AS source_id,
        %(status)s::pothole_status AS status,
        %(created_at)s::timestamptz AS created_at,
        %(completed_at)s::timestamptz AS completed_at,
        %(closed_outcome)s::closed_outcome AS closed_outcome,
        ST_SetSRID(ST_MakePoint(%(lng)s, %(lat)s), 4326)::geography AS location,
        %(street_address)s::text   AS street_address,
        %(city_ward)s::int         AS city_ward,
        %(raw)s::jsonb             AS raw
),
spatial_ward AS (
    SELECT i.*, (
        SELECT w.id FROM wards w
        WHERE ST_Contains(w.geometry::geometry, i.location::geometry)
        LIMIT 1
    ) AS spatial_ward_id
    FROM input i
)
INSERT INTO potholes (
    source_id, status, created_at, completed_at, closed_outcome,
    location, street_address, ward_id, raw,
    first_seen_at, last_synced_at, created_at_db, updated_at_db
)
SELECT
    source_id, status, created_at, completed_at, closed_outcome,
    location, street_address,
    COALESCE(spatial_ward_id, city_ward),
    raw,
    NOW(), NOW(), NOW(), NOW()
FROM spatial_ward
ON CONFLICT (source_id) DO UPDATE SET
    status          = EXCLUDED.status,
    completed_at    = EXCLUDED.completed_at,
    closed_outcome  = EXCLUDED.closed_outcome,
    -- location, ward_id, and street_address rarely change for an existing
    -- pothole, but the city does occasionally fix bad data — update them.
    location        = EXCLUDED.location,
    ward_id         = EXCLUDED.ward_id,
    street_address  = EXCLUDED.street_address,
    raw             = EXCLUDED.raw,
    last_synced_at  = NOW()
    -- first_seen_at and created_at_db are NOT updated — they're write-once
RETURNING (xmax = 0) AS was_insert;
"""


# Second pass: link duplicate_of by source_id lookup.
LINK_DUPLICATES_SQL = """
UPDATE potholes child
SET duplicate_of = parent.id,
    updated_at_db = NOW()
FROM potholes parent
WHERE parent.source_id = %(parent_source_id)s
  AND child.source_id  = %(child_source_id)s
  AND (child.duplicate_of IS NULL OR child.duplicate_of <> parent.id);
"""


# =============================================================
# Main ingest logic
# =============================================================
def ingest(
    backfill_since: Optional[str] = None,
    dry_run: bool = False,
    limit: Optional[int] = None,
) -> None:
    """
    Run the ingest. Either initial backfill or incremental, depending on args.
    """
    mode = (
        "backfill" if backfill_since
        else "dry_run" if dry_run
        else "incremental"
    )

    # In dry_run mode we don't even open a DB connection for the run.
    if dry_run:
        _run_dry(limit=limit, backfill_since=backfill_since)
        return

    # Real run — manage the ingest_runs lifecycle.
    with get_connection() as conn:
        with conn.cursor() as cur:
            run_id = start_ingest_run(cur, mode=mode)

            # Decide the filter window.
            created_after = backfill_since
            modified_after = None
            if not backfill_since:
                last_success = get_last_successful_run_start(cur)
                if last_success is None:
                    logger.error(
                        "No previous successful run found. Use --backfill --since YYYY-MM-DD "
                        "for the first load."
                    )
                    finish_ingest_run(
                        cur, run_id, "failed", 0, 0, 0,
                        "No previous run; --backfill required for first ingest",
                    )
                    sys.exit(1)
                modified_after = last_success.isoformat()
                logger.info("Incremental mode: fetching modified after %s", modified_after)

            # Commit the 'running' row + window-decision queries before the
            # long fetch begins. That way if we crash mid-fetch, ingest_runs
            # still shows the started run.
            conn.commit()

    # Fetch + normalize + upsert, in a fresh transaction.
    fetched = 0
    skipped = 0
    inserted = 0
    updated = 0
    duplicates_to_link: list[tuple[str, str]] = []  # (child_source_id, parent_source_id)

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                batch: list[dict[str, Any]] = []

                for raw in fetch_potholes(
                    created_after=created_after,
                    modified_after=modified_after,
                    max_records=limit,
                ):
                    fetched += 1
                    normalized = normalize_record(raw)
                    if normalized is None:
                        skipped += 1
                        continue

                    if normalized["parent_source_id"]:
                        duplicates_to_link.append(
                            (normalized["source_id"], normalized["parent_source_id"])
                        )

                    batch.append(normalized)
                    if len(batch) >= UPSERT_BATCH_SIZE:
                        ins, upd = _flush_batch(cur, batch, conn=conn)
                        inserted += ins
                        updated += upd
                        batch = []

                    if fetched % PROGRESS_LOG_INTERVAL == 0:
                        logger.info(
                            "Progress: fetched=%d inserted=%d updated=%d skipped=%d",
                            fetched, inserted, updated, skipped,
                        )

                # Flush the tail.
                if batch:
                    ins, upd = _flush_batch(cur, batch, conn=conn)
                    inserted += ins
                    updated += upd

                # Second pass: link duplicates.
                if duplicates_to_link:
                    logger.info("Linking %d duplicate relationships", len(duplicates_to_link))
                    execute_batch(
                        cur,
                        LINK_DUPLICATES_SQL,
                        [
                            {"child_source_id": child, "parent_source_id": parent}
                            for child, parent in duplicates_to_link
                        ],
                        page_size=500,
                    )
                
                # Refresh derived stats tables. Runs in the same transaction
                # so the ingest+refresh appear atomically to readers.
                logger.info("Refreshing derived stats...")
                refresh_all(cur)

                # Mark the run successful in the same transaction.
                finish_ingest_run(
                    cur, run_id, "success",
                    records_fetched=fetched,
                    records_added=inserted,
                    records_updated=updated,
                )

    except Exception as exc:
        logger.exception("Ingest failed")
        # New connection — the previous one is in a bad state due to the exception.
        with get_connection() as conn:
            with conn.cursor() as cur:
                finish_ingest_run(
                    cur, run_id, "failed",
                    records_fetched=fetched,
                    records_added=inserted,
                    records_updated=updated,
                    error_message=str(exc)[:2000],
                )
        raise

    logger.info(
        "Ingest complete: fetched=%d inserted=%d updated=%d skipped=%d",
        fetched, inserted, updated, skipped,
    )


def _flush_batch(
    cursor: Any,
    batch: list[dict[str, Any]],
    conn: Any = None,
) -> tuple[int, int]:
    
    inserted = 0
    updated = 0
    for record in batch:
        cursor.execute(
            UPSERT_POTHOLE_SQL,
            {
                "source_id":       record["source_id"],
                "status":          record["status"],
                "created_at":      record["created_at"],
                "completed_at":    record["completed_at"],
                "closed_outcome":  record["closed_outcome"],
                "lat":             record["lat"],
                "lng":             record["lng"],
                "street_address":  record["street_address"],
                "city_ward":       record["city_ward"],
                "raw":             Json(record["raw"]),
            },
        )
        was_insert = cursor.fetchone()[0]
        if was_insert:
            inserted += 1
        else:
            updated += 1
    if conn is not None:
        conn.commit()
    return inserted, updated


def _run_dry(limit: Optional[int], backfill_since: Optional[str]) -> None:
    """
    Dry run: fetch and normalize records but don't touch the database.
    Useful for sanity-checking the pipeline against real data.
    """
    fetched = 0
    skipped = 0
    samples: list[dict[str, Any]] = []
    duplicates = 0

    for raw in fetch_potholes(
        created_after=backfill_since,
        max_records=limit,
    ):
        fetched += 1
        normalized = normalize_record(raw)
        if normalized is None:
            skipped += 1
            continue
        if normalized["parent_source_id"]:
            duplicates += 1
        if len(samples) < 3:
            samples.append(normalized)

    logger.info("=" * 60)
    logger.info("DRY RUN COMPLETE")
    logger.info("Fetched:   %d", fetched)
    logger.info("Normalized: %d", fetched - skipped)
    logger.info("Skipped:   %d", skipped)
    logger.info("Duplicates (with parent_source_id): %d", duplicates)
    logger.info("=" * 60)
    for i, sample in enumerate(samples, 1):
        logger.info("Sample %d:", i)
        for key, value in sample.items():
            if key == "raw":
                continue  # too noisy to print
            logger.info("  %-18s %r", key, value)


# =============================================================
# CLI
# =============================================================
def main() -> None:
    parser = argparse.ArgumentParser(description="Load Chicago pothole records.")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Initial historical load. Requires --since.",
    )
    parser.add_argument(
        "--since",
        type=str,
        help="Backfill start date (YYYY-MM-DD). Used with --backfill.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch & normalize but don't write to the database.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cap on records fetched. Useful with --dry-run.",
    )
    args = parser.parse_args()

    if args.backfill and not args.since:
        parser.error("--backfill requires --since YYYY-MM-DD")
    if args.since and not (args.backfill or args.dry_run):
        parser.error("--since only makes sense with --backfill or --dry-run")

    ingest(
        backfill_since=args.since if (args.backfill or args.dry_run) else None,
        dry_run=args.dry_run,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()