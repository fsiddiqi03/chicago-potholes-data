"""
Socrata API client for the Chicago pothole dataset.

Wraps pagination, rate limiting, and Socrata-specific query syntax so the
loader can think in terms of 'give me records matching X' rather than HTTP.

The dataset id and base URL come from config.py. The pothole filter
(sr_short_code = 'PHF') is applied here, not by callers — this module
exists specifically to fetch potholes, not arbitrary 311 records.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Iterator, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import HTTP_TIMEOUT_SECONDS, SOCRATA_BASE_URL, WARDS_DATASET_ID, POTHOLE_SR_SHORT_CODE, POTHOLES_DATASET_ID, PAGE_SIZE, INTER_REQUEST_DELAY_SECONDS

logger = logging.getLogger(__name__)



def _build_session() -> requests.Session:
    """
    Build a requests Session with sensible retries.

    Socrata is reliable but transient 5xx errors happen. Three retries
    with exponential backoff handles 99% of those without us having to
    think about it. Retries do NOT apply to 4xx — those are our fault
    and retrying won't help.
    """
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1.0,        # 1s, 2s, 4s between retries
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session


def _normalize_socrata_timestamp(value: str) -> str:
    """
    Convert an ISO timestamp string to the format Socrata's SoQL expects:
    'YYYY-MM-DDTHH:MM:SS' (no timezone, no fractional seconds).

    The city's created_date and last_modified_date are timezone-naive
    'floating timestamps' in SoQL. Passing strings with timezone suffixes
    triggers a type-mismatch error. We strip both.

    Accepts:  '2026-05-21'                        -> '2026-05-21T00:00:00'
              '2026-05-21T22:35:38'               -> '2026-05-21T22:35:38'
              '2026-05-21T22:35:38.520401+00:00'  -> '2026-05-21T22:35:38'
    """
    from dateutil import parser as dateparser

    dt = dateparser.isoparse(value)
    # Drop tz and microseconds — Socrata wants 'YYYY-MM-DDTHH:MM:SS'.
    return dt.replace(tzinfo=None, microsecond=0).isoformat()


def _build_where_clause(
    created_after: Optional[str] = None,
    modified_after: Optional[str] = None,
) -> Optional[str]:
    """
    Construct a Socrata $where clause string from optional filters.

    Returns None if no filters apply.

    Both filter values must be ISO datetime strings. We normalize them
    to Socrata's expected format and wrap in cast() so SoQL treats them
    as timestamps rather than text.
    """
    clauses: list[str] = []
    if created_after:
        ts = _normalize_socrata_timestamp(created_after)
        clauses.append(f"created_date > '{ts}'")
    if modified_after:
        ts = _normalize_socrata_timestamp(modified_after)
        clauses.append(f"last_modified_date > '{ts}'")
    if not clauses:
        return None
    return " AND ".join(clauses)


def fetch_potholes(
    created_after: Optional[str] = None,
    modified_after: Optional[str] = None,
    page_size: int = PAGE_SIZE,
    max_records: Optional[int] = None,
) -> Iterator[dict[str, Any]]:
    """
    Yield pothole records from the Socrata API, one at a time.

    Lazy — paginates as the caller consumes. The caller can break out of
    the loop early (e.g., during a --dry-run test) and we'll stop fetching.

    Args:
        created_after:  ISO date string. Filters to rows created after this.
                        Use for backfill.
        modified_after: ISO date string. Filters to rows modified after this.
                        Use for incremental updates — catches status changes
                        on existing rows, not just newly-created tickets.
        page_size:      Records per HTTP request. Default 1000.
        max_records:    Hard cap on total yielded records. None = unlimited.
                        Useful for --dry-run testing.

    Yields:
        Raw record dicts straight from Socrata. Normalization happens
        downstream in transforms.pothole.normalize_record.
    """
    session = _build_session()
    where = _build_where_clause(created_after, modified_after)

    base_params: dict[str, Any] = {
        "sr_short_code": POTHOLE_SR_SHORT_CODE,
        "$limit": page_size,
        # Stable ordering is REQUIRED for correct pagination — without it,
        # rows can shift between pages and we'd miss or double-count records.
        # sr_number is unique and stable, so sorting by it is safe.
        "$order": "created_date ASC, sr_number ASC",
    }
    if where:
        base_params["$where"] = where

    url = f"{SOCRATA_BASE_URL}/{POTHOLES_DATASET_ID}.json"
    offset = 0
    yielded = 0

    logger.info(
        "Starting Socrata fetch: url=%s where=%r page_size=%d",
        url, where, page_size,
    )

    while True:
        params = {**base_params, "$offset": offset}

        logger.debug("Fetching page at offset %d", offset)
        response = session.get(url, params=params, timeout=HTTP_TIMEOUT_SECONDS)

        if response.status_code != 200:
            # 4xx: our query is wrong. Don't retry, surface the error.
            # 5xx already retried by the session; if we're here, retries gave up.
            raise RuntimeError(
                f"Socrata error {response.status_code} at offset {offset}: "
                f"{response.text[:500]}"
            )

        batch = response.json()
        if not isinstance(batch, list):
            raise RuntimeError(
                f"Unexpected Socrata response shape at offset {offset}: "
                f"{type(batch).__name__}"
            )

        if not batch:
            logger.info("Reached end of dataset at offset %d", offset)
            break

        for record in batch:
            yield record
            yielded += 1
            if max_records is not None and yielded >= max_records:
                logger.info("Hit max_records cap (%d), stopping", max_records)
                return

        logger.info(
            "Fetched page: offset=%d size=%d total_yielded=%d",
            offset, len(batch), yielded,
        )

        # If we got fewer records than requested, we've hit the end.
        if len(batch) < page_size:
            logger.info("Final page returned %d < %d, stopping", len(batch), page_size)
            break

        offset += page_size
        time.sleep(INTER_REQUEST_DELAY_SECONDS)

    logger.info("Socrata fetch complete: %d records yielded", yielded)