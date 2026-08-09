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
from datetime import date as DateType, timedelta
from typing import Any, Optional

from psycopg2.extras import Json

from ..db import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# NOTE for every SQL string in this module: keep literal percent signs out of
# them, comments included. psycopg2 scans the whole query for placeholders and
# does not skip SQL comments, so a stray one drops it into positional binding
# and any dict of params fails with "dict is not a sequence". Write "a third"
# rather than "33 pct", or escape it as a doubled percent sign.


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

# Intentionally includes dup_open, unlike every other query here. This is the
# latest *report*, and a duplicate ticket is a real person reporting a real
# pothole. Counting queries exclude duplicates because two tickets on one
# pothole would double-count the backlog; a "most recent report" doesn't.
# Don't "fix" this to match the others.
SQL_LATEST_OPEN = """
SELECT
    id,
    source_id,
    created_at,
    street_address,
    ward_id,
    ST_Y(location::geometry) AS lat,
    ST_X(location::geometry) AS lng
FROM potholes
WHERE status IN ('open', 'dup_open')
ORDER BY created_at DESC
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
    -- completed_30d counts everything that left the queue, duplicate
    -- cleanup included. repaired_30d is the subset that was actual
    -- asphalt — use it anywhere the UI says "fixed".
    count(*) FILTER (
        WHERE closed_outcome = 'repaired'
          AND completed_at >= now() - interval '30 days'
    ) AS repaired_30d,
    round(
        avg(extract(epoch FROM completed_at - created_at) / 86400) FILTER (
            WHERE closed_outcome = 'repaired'
              AND completed_at >= now() - interval '30 days'
        )::numeric,
        1
    ) AS avg_days_to_fix_30d
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

    # --- latest_open_report ---
    cursor.execute(SQL_LATEST_OPEN)
    row = cursor.fetchone()
    if row is not None:
        cols = [d[0] for d in cursor.description]
        latest = dict(zip(cols, row))
        latest["id"] = str(latest["id"])
        latest["created_at"] = latest["created_at"].isoformat()
        cursor.execute(
            UPSERT_DASHBOARD_CACHE,
            {"key": "latest_open_report", "value": Json(latest)},
        )
        logger.info(
            "  latest_open_report: %s, created %s, ward %s",
            latest["source_id"], latest["created_at"], latest["ward_id"],
        )
    else:
        logger.warning("  No open reports found — skipping latest_open_report")
        
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
# Creation-cohort window for median_days_to_resolve. Every ward is measured
# over the same window, so the number is comparable across wards but capped
# by it: a pothole open the whole time contributes at most this many days.
# Shorter reacts faster to current performance; longer truncates less. At 90
# the worst-ten list is stable against a 90-180d window (7/10 overlap), while
# mid-table wards move ~8 places — rank the extremes, don't over-read the middle.
RESOLUTION_COHORT_DAYS = 90

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
    -- closed_count is every ticket that left the queue on this date;
    -- repaired_count is the subset that was actual asphalt. Over a third of
    -- closures are duplicate cleanup, so reporting closed_count as "work
    -- done" overstates a ward's output substantially.
    SELECT
        ward_id,
        count(*) AS closed_count,
        count(*) FILTER (WHERE closed_outcome = 'repaired') AS repaired_count
    FROM potholes
    WHERE completed_at::date = %(target_date)s AND ward_id IS NOT NULL
    GROUP BY ward_id
),
resolution AS (
    -- Time-to-resolution across the WHOLE creation cohort, not just the
    -- tickets that happened to close. Still-open potholes are counted at
    -- their current age (right-censored), so a ward that closes nothing
    -- sees this number climb every day instead of vanishing from the
    -- ranking for lack of data. This is the ranking metric; the
    -- recent_repairs median below is context only.
    SELECT
        ward_id,
        count(*)                                  AS cohort_n,
        count(*) FILTER (WHERE completed_at IS NULL) AS still_open_n,
        percentile_cont(0.5) WITHIN GROUP (
            ORDER BY extract(
                epoch FROM (coalesce(completed_at, now()) - created_at)
            ) / 86400
        ) AS median_days_to_resolve
    FROM potholes
    WHERE ward_id IS NOT NULL
      -- Duplicates and cancellations aren't work; excluding both means
      -- this can't be gamed by reclassifying a backlog.
      AND status IN ('open', 'completed')
      AND created_at >= now() - make_interval(days => %(cohort_days)s)
    GROUP BY ward_id
),
recent_repairs AS (
    SELECT
        ward_id,
        -- Doubles as a throughput metric ("repairs completed, 30d") and as
        -- the sample size behind median_days_to_fix. The latter matters:
        -- low-volume wards close 1-2 potholes a month, and percentile_cont
        -- over n=1 is just that one ticket's duration. Never rank on
        -- median_days_to_fix — it only sees potholes that got fixed, so a
        -- ward that repairs nothing has no data rather than a bad score.
        -- median_days_to_resolve above is the ranking metric.
        count(*) AS repair_sample_n,
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
    coalesce(c.repaired_count, 0)          AS repaired_count,
    round(o.avg_days_open::numeric, 2)     AS avg_days_open,
    round(r.median_days_to_fix::numeric, 2) AS median_days_to_fix,
    coalesce(r.repair_sample_n, 0)         AS repair_sample_n,
    round(res.median_days_to_resolve::numeric, 1) AS median_days_to_resolve,
    coalesce(res.cohort_n, 0)              AS cohort_n,
    coalesce(res.still_open_n, 0)          AS still_open_n,
    round(o.pct_over_sla::numeric, 2)      AS pct_over_sla
FROM wards w
LEFT JOIN open_stats     o   ON o.ward_id   = w.id
LEFT JOIN closed_on_date c   ON c.ward_id   = w.id
LEFT JOIN recent_repairs r   ON r.ward_id   = w.id
LEFT JOIN resolution     res ON res.ward_id = w.id
ORDER BY w.id;
"""

UPSERT_WARD_DAILY_STATS = """
INSERT INTO ward_daily_stats (
    ward_id, date, open_count, closed_count, repaired_count,
    avg_days_open, median_days_to_fix, repair_sample_n,
    median_days_to_resolve, cohort_n, still_open_n, pct_over_sla
)
VALUES (
    %(ward_id)s, %(date)s, %(open_count)s, %(closed_count)s, %(repaired_count)s,
    %(avg_days_open)s, %(median_days_to_fix)s, %(repair_sample_n)s,
    %(median_days_to_resolve)s, %(cohort_n)s, %(still_open_n)s, %(pct_over_sla)s
)
ON CONFLICT (ward_id, date) DO UPDATE SET
    open_count             = EXCLUDED.open_count,
    closed_count           = EXCLUDED.closed_count,
    repaired_count         = EXCLUDED.repaired_count,
    avg_days_open          = EXCLUDED.avg_days_open,
    median_days_to_fix     = EXCLUDED.median_days_to_fix,
    repair_sample_n        = EXCLUDED.repair_sample_n,
    median_days_to_resolve = EXCLUDED.median_days_to_resolve,
    cohort_n               = EXCLUDED.cohort_n,
    still_open_n           = EXCLUDED.still_open_n,
    pct_over_sla           = EXCLUDED.pct_over_sla;
"""


# closed_count / repaired_count are the only genuinely date-scoped columns in
# ward_daily_stats — everything else is a now()-relative snapshot. The last
# run of a given day fires hours before the day ends (~6% of Chicago closures
# land after it), and the next run has already advanced to a new date, so a
# day's counts are permanently short unless we come back for them.
#
# This re-counts ONLY those two columns for an already-past date. It
# deliberately does not touch the snapshot columns: those were correct as of
# when they were written, and recomputing them now would stamp today's
# numbers onto yesterday's row.
SQL_FINALIZE_DAY_COUNTS = """
UPDATE ward_daily_stats wds
SET closed_count   = c.closed_count,
    repaired_count = c.repaired_count
FROM wards w
LEFT JOIN LATERAL (
    SELECT
        count(*) AS closed_count,
        count(*) FILTER (WHERE closed_outcome = 'repaired') AS repaired_count
    FROM potholes p
    WHERE p.ward_id = w.id
      AND p.completed_at::date = %(target_date)s
) c ON true
WHERE wds.ward_id = w.id
  AND wds.date = %(target_date)s
  AND (wds.closed_count   IS DISTINCT FROM c.closed_count
    OR wds.repaired_count IS DISTINCT FROM c.repaired_count);
"""


def finalize_prior_day(cursor: Any, target_date: DateType) -> None:
    """
    Re-count the day before target_date so its closure totals are final.

    No-ops if that date has no rows (a gap in runs) — the snapshot columns
    can't be reconstructed after the fact, so we'd rather leave the day
    missing than write a row that's half real and half fabricated.
    """
    prior = target_date - timedelta(days=1)
    cursor.execute(SQL_FINALIZE_DAY_COUNTS, {"target_date": prior})
    logger.info("  Finalized closure counts for %s (%d rows changed)",
                prior, cursor.rowcount)


def refresh_ward_daily_stats(
    cursor: Any,
    target_date: Optional[DateType] = None,
) -> None:
    """
    Compute and upsert ward_daily_stats for the given date (defaults to today).

    Always writes exactly 50 rows (one per ward), even for wards with no
    activity — those get zeros and nulls. Easier for the frontend to consume
    50 known rows than to handle missing wards.

    Nothing here is ever filtered out for thin data. A ward that repairs
    almost nothing is the most important row on an accountability dashboard,
    and any server-side minimum would null its median and drop it from the
    leaderboard entirely. Rank on median_days_to_resolve, which counts every
    pothole in the cohort including the ones still sitting open.
    """
    if target_date is None:
        # Use the DB's notion of 'today' for consistency across runs.
        cursor.execute("SELECT current_date;")
        (target_date,) = cursor.fetchone()

    logger.info("Refreshing ward_daily_stats for %s...", target_date)

    cursor.execute(
        SQL_WARD_DAILY_STATS,
        {"target_date": target_date, "cohort_days": RESOLUTION_COHORT_DAYS},
    )
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

    # The previous day stopped being written to the moment the date rolled
    # over, mid-afternoon Chicago time. Go back and settle its counts.
    finalize_prior_day(cursor, target_date)


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