"""
Refresh derived stats tables (dashboard_cache, ward_daily_stats) from the
potholes source table.

Designed to run after every ingest, OR standalone via:
    python -m src.transforms.stats
    python -m src.transforms.stats --date 2026-05-21    (specific date)

Idempotent: re-running for the same date overwrites that date's row.
Historical rows (other dates) are never touched.
"""
from __future__ import annotations

import argparse
import json
import logging
from datetime import date as DateType
from typing import Any, Optional

from psycopg2.extras import Json

from ..db import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# =============================================================
# dashboard_cache refresh
# =============================================================
SQL_OLDEST_OPEN = """
SELECT
    id,
    source_id,
    created_at,
    street_address,
    ward_id,
    ST_Y(location::geometry) AS lat,
    ST_X(location::geometry) AS lng
FROM potholes
WHERE status = 'open'
ORDER BY created_at ASC
LIMIT 1;
"""

SQL_SLA_BREACH_COUNT = """
SELECT count(*) AS breaches
FROM potholes
WHERE status = 'open'
  AND created_at < now() - interval '7 days';
"""

SQL_CITY_SUMMARY = """
SELECT
    count(*) FILTER (WHERE status = 'open') AS total_open,
    count(*) FILTER (
        WHERE status IN ('completed', 'dup_closed')
          AND completed_at >= now() - interval '30 days'
    ) AS completed_30d,
    round(
        avg(extract(epoch FROM completed_at - created_at) / 86400) FILTER (
            WHERE closed_outcome = 'repaired'
              AND completed_at >= now() - interval '30 days'
        )::numeric,
        1
    ) AS avg_days_to_fix_30d,
    count(*) FILTER (
        WHERE closed_outcome = 'no_pothole_found'
          AND completed_at >= now() - interval '30 days'
    ) AS closed_no_pothole_30d
FROM potholes;
"""

UPSERT_DASHBOARD_CACHE = """
INSERT INTO dashboard_cache (key, value, updated_at)
VALUES (%(key)s, %(value)s, NOW())
ON CONFLICT (key) DO UPDATE SET
    value      = EXCLUDED.value,
    updated_at = NOW();
"""


def refresh_dashboard_cache(cursor: Any) -> None:
    """Recompute and upsert the homepage cache keys."""
    logger.info("Refreshing dashboard_cache...")

    # --- oldest_open_pothole ---
    cursor.execute(SQL_OLDEST_OPEN)
    row = cursor.fetchone()
    if row is not None:
        # cursor.description gives us column names in the same order as row values.
        cols = [d[0] for d in cursor.description]
        oldest = dict(zip(cols, row))
        # Cast non-JSON-serializable types.
        oldest["id"] = str(oldest["id"])
        oldest["created_at"] = oldest["created_at"].isoformat()
        cursor.execute(
            UPSERT_DASHBOARD_CACHE,
            {"key": "oldest_open_pothole", "value": Json(oldest)},
        )
        logger.info(
            "  oldest_open_pothole: %s, created %s, ward %s",
            oldest["source_id"], oldest["created_at"], oldest["ward_id"],
        )
    else:
        logger.warning("  No open potholes found — skipping oldest_open_pothole")

    # --- sla_breach_count ---
    cursor.execute(SQL_SLA_BREACH_COUNT)
    (breaches,) = cursor.fetchone()
    cursor.execute(
        UPSERT_DASHBOARD_CACHE,
        {
            "key": "sla_breach_count",
            "value": Json({"count": breaches, "sla_days": 7}),
        },
    )
    logger.info("  sla_breach_count: %d", breaches)

    # --- city_summary ---
    cursor.execute(SQL_CITY_SUMMARY)
    row = cursor.fetchone()
    cols = [d[0] for d in cursor.description]
    summary = dict(zip(cols, row))
    # numeric -> float for JSON serialization
    if summary.get("avg_days_to_fix_30d") is not None:
        summary["avg_days_to_fix_30d"] = float(summary["avg_days_to_fix_30d"])
    cursor.execute(
        UPSERT_DASHBOARD_CACHE,
        {"key": "city_summary", "value": Json(summary)},
    )
    logger.info("  city_summary: %s", json.dumps(summary, default=str))


# =============================================================
# ward_daily_stats refresh
# =============================================================
SQL_WARD_DAILY_STATS = """
WITH open_stats AS (
    SELECT
        ward_id,
        count(*) AS open_count,
        avg(extract(epoch FROM (now() - created_at)) / 86400) AS avg_days_open,
        100.0 * count(*) FILTER (WHERE created_at < now() - interval '7 days')
             / count(*) AS pct_over_sla
    FROM potholes
    WHERE status = 'open' AND ward_id IS NOT NULL
    GROUP BY ward_id
),
closed_on_date AS (
    SELECT ward_id, count(*) AS closed_count
    FROM potholes
    WHERE completed_at::date = %(target_date)s AND ward_id IS NOT NULL
    GROUP BY ward_id
),
recent_repairs AS (
    SELECT
        ward_id,
        percentile_cont(0.5) WITHIN GROUP (
            ORDER BY extract(epoch FROM completed_at - created_at) / 86400
        ) AS median_days_to_fix
    FROM potholes
    WHERE closed_outcome = 'repaired'
      AND completed_at >= now() - interval '30 days'
      AND ward_id IS NOT NULL
    GROUP BY ward_id
)
SELECT
    w.id AS ward_id,
    coalesce(o.open_count, 0)              AS open_count,
    coalesce(c.closed_count, 0)            AS closed_count,
    round(o.avg_days_open::numeric, 2)     AS avg_days_open,
    round(r.median_days_to_fix::numeric, 2) AS median_days_to_fix,
    round(o.pct_over_sla::numeric, 2)      AS pct_over_sla
FROM wards w
LEFT JOIN open_stats     o ON o.ward_id = w.id
LEFT JOIN closed_on_date c ON c.ward_id = w.id
LEFT JOIN recent_repairs r ON r.ward_id = w.id
ORDER BY w.id;
"""

UPSERT_WARD_DAILY_STATS = """
INSERT INTO ward_daily_stats (
    ward_id, date, open_count, closed_count,
    avg_days_open, median_days_to_fix, pct_over_sla
)
VALUES (
    %(ward_id)s, %(date)s, %(open_count)s, %(closed_count)s,
    %(avg_days_open)s, %(median_days_to_fix)s, %(pct_over_sla)s
)
ON CONFLICT (ward_id, date) DO UPDATE SET
    open_count          = EXCLUDED.open_count,
    closed_count        = EXCLUDED.closed_count,
    avg_days_open       = EXCLUDED.avg_days_open,
    median_days_to_fix  = EXCLUDED.median_days_to_fix,
    pct_over_sla        = EXCLUDED.pct_over_sla;
"""


def refresh_ward_daily_stats(
    cursor: Any,
    target_date: Optional[DateType] = None,
) -> None:
    """
    Compute and upsert ward_daily_stats for the given date (defaults to today).

    Always writes exactly 50 rows (one per ward), even for wards with no
    activity — those get zeros and nulls. Easier for the frontend to consume
    50 known rows than to handle missing wards.
    """
    if target_date is None:
        # Use the DB's notion of 'today' for consistency across runs.
        cursor.execute("SELECT current_date;")
        (target_date,) = cursor.fetchone()

    logger.info("Refreshing ward_daily_stats for %s...", target_date)

    cursor.execute(SQL_WARD_DAILY_STATS, {"target_date": target_date})
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description]

    if not rows:
        logger.warning("  No rows returned — wards table empty?")
        return

    for row in rows:
        record = dict(zip(cols, row))
        record["date"] = target_date
        cursor.execute(UPSERT_WARD_DAILY_STATS, record)

    logger.info("  Wrote %d ward stats rows for %s", len(rows), target_date)


# =============================================================
# Orchestrator
# =============================================================
def refresh_all(cursor: Any, target_date: Optional[DateType] = None) -> None:
    """Run both refreshes in sequence. Called from the loader after ingest."""
    refresh_dashboard_cache(cursor)
    refresh_ward_daily_stats(cursor, target_date=target_date)


# =============================================================
# CLI
# =============================================================
def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh derived stats tables.")
    parser.add_argument(
        "--date",
        type=str,
        help="Target date for ward_daily_stats (YYYY-MM-DD). Defaults to today.",
    )
    args = parser.parse_args()

    target_date: Optional[DateType] = None
    if args.date:
        from datetime import date as _date_class
        target_date = _date_class.fromisoformat(args.date)

    with get_connection() as conn:
        with conn.cursor() as cur:
            refresh_all(cur, target_date=target_date)

    logger.info("Stats refresh complete.")


if __name__ == "__main__":
    main()