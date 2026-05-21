"""
Load Chicago ward boundaries from the city data portal into the `wards` table.

Idempotent: safe to run repeatedly. Uses INSERT ... ON CONFLICT to update
existing wards rather than duplicating them. Geometry is converted from
GeoJSON to PostGIS geography(MultiPolygon, 4326) at insert time.

Usage:
    python -m src.loaders.load_wards
"""
import json
import logging
import sys
from typing import Any

import requests

from ..config import HTTP_TIMEOUT_SECONDS, SOCRATA_BASE_URL, WARDS_DATASET_ID
from ..db import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def fetch_wards_geojson() -> dict[str, Any]:
    """Fetch the ward boundaries dataset as GeoJSON from the Socrata API."""
    url = f"{SOCRATA_BASE_URL}/{WARDS_DATASET_ID}.geojson"
    logger.info("Fetching ward boundaries from %s", url)

    response = requests.get(url, timeout=HTTP_TIMEOUT_SECONDS)
    response.raise_for_status()

    data = response.json()
    features = data.get("features", [])
    logger.info("Received %d ward features", len(features))

    if len(features) != 50:
        # Chicago has 50 wards. If we get a different count, something
        # is wrong with the dataset or our assumptions -- fail loudly.
        logger.warning(
            "Expected 50 wards, got %d. Continuing anyway, but check the dataset.",
            len(features),
        )

    return data


def extract_ward_number(properties: dict[str, Any]) -> int:
    """
    Pull the ward number out of a GeoJSON feature's properties dict.

    The city sometimes uses 'ward' as a string ("1", "2", ...) -- coerce to int.
    Defensive against field name variations across dataset versions.
    """
    for field in ("ward", "WARD", "ward_id", "ward_num"):
        if field in properties:
            value = properties[field]
            if value is not None:
                return int(value)
    raise ValueError(
        f"Could not find ward number in properties: {list(properties.keys())}"
    )


def upsert_ward(
    cursor: Any,
    ward_number: int,
    geometry_geojson: dict[str, Any],
) -> str:
    """
    Insert or update a single ward. Returns 'inserted' or 'updated' for stats.

    The geometry conversion chain is important -- annotated below.
    """
    # ST_GeomFromGeoJSON parses GeoJSON into PostGIS geometry.
    # The chained calls do, in order:
    #   1. ST_GeomFromGeoJSON(%s)   -> geometry, SRID may be unset
    #   2. ST_SetSRID(..., 4326)    -> force SRID 4326 (WGS 84 lat/lng)
    #   3. ST_Multi(...)            -> coerce Polygon to MultiPolygon
    #      (our column is multipolygon; some wards may come as plain Polygon)
    #   4. ::geography              -> cast to geography type
    sql = """
        INSERT INTO wards (id, name, geometry, created_at, updated_at)
        VALUES (
            %(id)s,
            %(name)s,
            ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(%(geom)s), 4326))::geography,
            NOW(),
            NOW()
        )
        ON CONFLICT (id) DO UPDATE SET
            name       = EXCLUDED.name,
            geometry   = EXCLUDED.geometry,
            updated_at = NOW()
        RETURNING (xmax = 0) AS inserted;
    """
    # The `xmax = 0` trick is a Postgres idiom: xmax is 0 for freshly
    # inserted rows, non-zero for rows that were updated. Lets us tell
    # inserts from updates without a separate query.

    cursor.execute(
        sql,
        {
            "id": ward_number,
            "name": f"Ward {ward_number}",
            "geom": json.dumps(geometry_geojson),
        },
    )
    inserted = cursor.fetchone()[0]
    return "inserted" if inserted else "updated"


def load_wards() -> None:
    """Main entry point. Fetch, parse, upsert, report."""
    geojson = fetch_wards_geojson()
    features = geojson.get("features", [])

    if not features:
        logger.error("No ward features in response. Aborting.")
        sys.exit(1)

    inserted_count = 0
    updated_count = 0
    skipped_count = 0

    with get_connection() as conn:
        with conn.cursor() as cur:
            for feature in features:
                try:
                    ward_number = extract_ward_number(feature["properties"])
                    geometry = feature["geometry"]

                    if geometry is None:
                        logger.warning(
                            "Ward %s has no geometry. Skipping.", ward_number
                        )
                        skipped_count += 1
                        continue

                    result = upsert_ward(cur, ward_number, geometry)
                    if result == "inserted":
                        inserted_count += 1
                        logger.info("Ward %d: inserted", ward_number)
                    else:
                        updated_count += 1
                        logger.info("Ward %d: updated", ward_number)

                except Exception as exc:
                    # Log and continue rather than aborting the whole load.
                    # One bad ward shouldn't kill the other 49.
                    logger.exception(
                        "Failed to load ward feature: %s. Skipping.", exc
                    )
                    skipped_count += 1

    logger.info(
        "Done. inserted=%d updated=%d skipped=%d total=%d",
        inserted_count,
        updated_count,
        skipped_count,
        len(features),
    )


if __name__ == "__main__":
    load_wards()
