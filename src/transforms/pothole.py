"""
Transform city pothole records (Socrata API shape) into our schema shape.

Pure functions, no I/O. Each function takes a raw dict from the city and
returns either a normalized dict or None (to indicate "skip this row").

This module never raises on data quality issues — instead it returns None
and the caller logs a skip. We do this so a single bad row never aborts a
batch of thousands.
"""
from __future__ import annotations

import logging
from typing import Any, Optional
from zoneinfo import ZoneInfo

from dateutil import parser as dateparser

logger = logging.getLogger(__name__)

# Socrata's created_date / closed_date are Chicago-local wall-clock times
# carrying no offset. We stamp them with this zone so they store as the
# correct instant. ZoneInfo (not a fixed offset) so CST/CDT is handled.
_CHICAGO_TZ = ZoneInfo("America/Chicago")


# Maps the city's (status, duplicate) tuple to our pothole_status enum value.
# Canceled overrides the duplicate flag — canceled is canceled regardless.
_STATUS_MAP: dict[tuple[str, bool], str] = {
    ("Open",      False): "open",
    ("Open",      True):  "dup_open",
    ("Completed", False): "completed",
    ("Completed", True):  "dup_closed",
}


def _parse_timestamp(value: Optional[str]) -> Optional[str]:
    """
    Parse a Socrata timestamp string into an ISO 8601 string suitable for
    insertion as timestamptz. Returns None for missing/empty values.

    Socrata returns timestamps like '2026-05-21T16:17:57.000' — naive ISO
    without timezone. They are actually America/Chicago local time (the
    city's own clock). We attach that zone so the value carries an explicit
    offset (e.g. -05:00), which makes ::timestamptz store the correct
    instant regardless of the DB session's timezone. Relying on the session
    timezone is not viable: poolers (e.g. PgBouncer) silently drop the
    startup `timezone` option, leaving the session at UTC.
    """
    if not value:
        return None
    try:
        # dateparser.isoparse is permissive of Socrata's variations.
        dt = dateparser.isoparse(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_CHICAGO_TZ)
        return dt.isoformat()
    except (ValueError, TypeError):
        return None


def _parse_float(value: Any) -> Optional[float]:
    """Coerce a string/number to float. Return None on failure."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _parse_int(value: Any) -> Optional[int]:
    """Coerce a string/number to int. Return None on failure."""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _classify_status(raw_status: Optional[str], duplicate: bool) -> Optional[str]:
    """Map city status + duplicate flag to our enum value."""
    if not raw_status:
        return None
    if raw_status == "Canceled":
        return "canceled"
    return _STATUS_MAP.get((raw_status, bool(duplicate)))


def _infer_closed_outcome(
    status: str,
    completed_at: Optional[str],
    duplicate: bool,
) -> str:
    """
    Classify WHY a ticket was closed.

    The city marks everything 'Completed' but the reality is several things:
    a real repair, an inspection that found nothing, or a duplicate. The
    current 311 dataset (v6vf-nfxy) publishes no outcome field, so anything
    beyond duplicate/canceled has to be inferred.

    Rules (in order):
      - canceled status        -> 'other' (canceled isn't really a closure)
      - duplicate flag is true -> 'duplicate'
      - status not completed   -> 'unknown' (still open)
      - no closed_date         -> 'unknown' (completed but undated; don't guess)
      - otherwise              -> 'repaired'

    We deliberately do NOT emit 'no_pothole_found'. An earlier version treated
    a sub-24h close as "too fast to be a real repair" — that is backwards. The
    legacy dataset (7as2-ds3y) carries ground-truth outcome labels alongside
    timestamps, and over 50k closed PHF records it says the opposite:

        closed < 24h:  95.3% 'Pothole Patched',  1.3% 'No Potholes Found'
        closed 1d+:    77.1% 'Pothole Patched',  8.6% 'No Potholes Found'

    ('number_of_potholes_filled_on_block' agrees: 95.2% vs 76.4% nonzero.)

    Fast closes are crews already on the block patching and closing on the
    spot, i.e. the *most* reliably-real repairs. With no published outcome
    field there is no signal left that distinguishes a no-find, so we don't
    pretend there is one.
    """
    if status == "canceled":
        return "other"
    if duplicate:
        return "duplicate"
    if status not in ("completed", "dup_closed"):
        return "unknown"
    if not completed_at:
        return "unknown"
    return "repaired"


def normalize_record(raw: dict[str, Any]) -> Optional[dict[str, Any]]:
    """
    Normalize one Socrata pothole record into a dict matching our schema.

    Returns None (and logs a skip reason) if the record can't be ingested.
    The caller increments a 'skipped' counter and moves on.

    The returned dict has all the fields the upsert SQL expects, with
    types that map cleanly to psycopg2 placeholders.
    """
    source_id = raw.get("sr_number")
    if not source_id:
        logger.debug("Skipping record with no sr_number")
        return None

    # Coordinates are required. Modern data has them; historical sometimes doesn't.
    lat = _parse_float(raw.get("latitude"))
    lng = _parse_float(raw.get("longitude"))
    if lat is None or lng is None:
        logger.debug("Skipping %s: missing coordinates", source_id)
        return None

    # Sanity bounds: Chicago is roughly 41.6–42.05 lat, -87.95 to -87.5 lng.
    # Anything wildly outside this is bad data — don't trust it.
    if not (41.5 <= lat <= 42.1 and -88.0 <= lng <= -87.4):
        logger.debug(
            "Skipping %s: coords outside Chicago bounds (%s, %s)",
            source_id, lat, lng,
        )
        return None

    duplicate = bool(raw.get("duplicate"))
    status = _classify_status(raw.get("status"), duplicate)
    if status is None:
        logger.debug(
            "Skipping %s: unknown status %r",
            source_id, raw.get("status"),
        )
        return None

    created_at = _parse_timestamp(raw.get("created_date"))
    if created_at is None:
        logger.debug("Skipping %s: no created_date", source_id)
        return None

    completed_at = _parse_timestamp(raw.get("closed_date"))
    closed_outcome = _infer_closed_outcome(
        status=status,
        completed_at=completed_at,
        duplicate=duplicate,
    )

    return {
        "source_id":           source_id,
        "status":              status,
        "created_at":          created_at,
        "completed_at":        completed_at,
        "closed_outcome":      closed_outcome,
        "lat":                 lat,
        "lng":                 lng,
        "street_address":      raw.get("street_address"),
        "city_ward":           _parse_int(raw.get("ward")),
        "parent_source_id":    raw.get("parent_sr_number"),  # for duplicate linking pass 2
        "raw":                 raw,  # full record preserved for future-proofing
    }