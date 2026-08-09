"""
Rebuild historical ward_daily_stats rows.

Existing history was written by an older refresh that computed every column
against now(), so a row stamped with a past date actually holds a snapshot
from whenever that run happened to fire. Those rows also predate the
closed_outcome reclassification, so their repair metrics were computed while
the fastest repairs were mislabeled and excluded.

This walks a date range and rewrites each day using the SAME query the live
4-hourly refresh uses, parameterized to an as-of instant. Backfilled and live
rows therefore share one definition and a trend chart has no seam.

Run with:
    python -m src.transforms.backfill_stats --from 2026-01-01 --to 2026-08-08
    python -m src.transforms.backfill_stats --from 2026-01-01          (to yesterday)
    python -m src.transforms.backfill_stats --from 2026-01-01 --dry-run

Idempotent: re-running the same range overwrites the same rows.
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date as DateType, timedelta
from typing import Any, Optional

from ..db import get_connection
from .stats import refresh_ward_daily_stats

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# Commit every N days so a long run doesn't hold one enormous transaction,
# and so an interruption leaves completed days durably written.
COMMIT_EVERY_DAYS = 15


# A backfill start date has to reflect where our data actually becomes dense,
# not where the oldest row sits. Incremental syncs pick up individual ancient
# tickets whenever the city touches them, so min(created_at) can predate real
# coverage by years while holding a literal handful of rows. Reconstructing
# those months yields an open_count near zero that ramps up as coverage
# arrives — a fake trend, and a very convincing looking one.
#
# So: find the first month whose volume is a meaningful fraction of typical
# monthly volume. Stragglers are orders of magnitude below the median and
# fall out; the real start survives.
COVERAGE_MIN_FRACTION = 0.10

SQL_COVERAGE_START = """
WITH monthly AS (
    SELECT date_trunc('month', created_at)::date AS m, count(*) AS n
    FROM potholes
    GROUP BY 1
),
med AS (
    SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY n) AS mid FROM monthly
)
SELECT min(monthly.m)
FROM monthly, med
WHERE monthly.n >= %(fraction)s * med.mid;
"""


def get_coverage_start(cursor: Any) -> Optional[DateType]:
    """
    First date our ingest holds data densely enough to reconstruct.

    Returns None only if the potholes table is empty.
    """
    cursor.execute(SQL_COVERAGE_START, {"fraction": COVERAGE_MIN_FRACTION})
    row = cursor.fetchone()
    return row[0] if row else None


def backfill(
    start: DateType,
    end: DateType,
    dry_run: bool = False,
) -> None:
    """Rewrite ward_daily_stats for every date in [start, end]."""
    if start > end:
        logger.error("--from (%s) is after --to (%s)", start, end)
        sys.exit(1)

    total_days = (end - start).days + 1

    with get_connection() as conn:
        with conn.cursor() as cur:
            coverage = get_coverage_start(cur)
            if coverage is None:
                logger.error("potholes table is empty — nothing to rebuild.")
                sys.exit(1)

            logger.info("Ingest coverage begins %s", coverage)
            if start < coverage:
                logger.warning(
                    "--from %s precedes our data (%s). Days before that would "
                    "show a fake ramp-up as coverage fills in, not real history. "
                    "Clamping to %s.",
                    start, coverage, coverage,
                )
                start = coverage
                total_days = (end - start).days + 1

            logger.info(
                "Rebuilding %d day(s): %s .. %s%s",
                total_days, start, end, "  [DRY RUN]" if dry_run else "",
            )
            if dry_run:
                logger.info("Dry run — no writes. Exiting.")
                return

            began = time.monotonic()
            current = start
            done = 0

            while current <= end:
                # finalize_prior=False: the next iteration rewrites that day
                # in full, so settling its counts here would be wasted work.
                refresh_ward_daily_stats(
                    cur, target_date=current, finalize_prior=False,
                )
                done += 1
                current += timedelta(days=1)

                if done % COMMIT_EVERY_DAYS == 0:
                    conn.commit()
                    elapsed = time.monotonic() - began
                    rate = done / elapsed
                    remaining = (total_days - done) / rate if rate else 0
                    logger.info(
                        "Progress: %d/%d days (%.1f%%) — %.1fs elapsed, ~%.0fs left",
                        done, total_days, 100.0 * done / total_days,
                        elapsed, remaining,
                    )

            conn.commit()
            logger.info(
                "Backfill complete: %d days rewritten in %.1fs",
                done, time.monotonic() - began,
            )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Rebuild historical ward_daily_stats rows.",
    )
    parser.add_argument(
        "--from", dest="start", type=DateType.fromisoformat, required=True,
        help="First date to rebuild (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--to", dest="end", type=DateType.fromisoformat, default=None,
        help="Last date to rebuild (YYYY-MM-DD). Defaults to yesterday.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Report the range and coverage, then exit without writing.",
    )
    args = parser.parse_args()

    # Default to yesterday: today's row is owned by the live refresh, and
    # rewriting it here would just duplicate work the next ingest redoes.
    end = args.end or (DateType.today() - timedelta(days=1))

    backfill(start=args.start, end=end, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
