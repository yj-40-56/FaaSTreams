"""
Running analytical queries against the 'ais' table.
Accepts typed query param model classes, returns typed result models.
"""

import duckdb
import pandas as pd

from worker.models import (
    BoundingBox,
    TimeWindow,
    ZoneIntrusionParams,
    VesselPing,
    VesselSummary,
)


def vessels_in_bbox(
    conn: duckdb.DuckDBPyConnection,
    bbox: BoundingBox,
) -> list[VesselPing]:
    """
    SPATIAL query.

    Returns every AIS ping recorded inside the given bounding box,
    regardless of when it was recorded.

    Useful for: port area monitoring, strait surveillance, zone inventories.
    Next step: replace bbox with ST_Within(geom, polygon) for arbitrary shapes.
    """
    rows = conn.execute("""
        SELECT
            mmsi, name, ship_type, nav_status,
            lat, lon, sog, ts AS timestamp
        FROM ais
        WHERE lat BETWEEN ? AND ?
          AND lon BETWEEN ? AND ?
        ORDER BY ts DESC
    """, [bbox.lat_min, bbox.lat_max, bbox.lon_min, bbox.lon_max]).fetchall()

    columns = ["mmsi", "name", "ship_type", "nav_status", "lat", "lon", "sog", "timestamp"]
    return [VesselPing(**dict(zip(columns, row))) for row in rows]


def vessel_activity_in_window(
    conn: duckdb.DuckDBPyConnection,
    window: TimeWindow,
) -> list[VesselSummary]:
    """
    TEMPORAL query.

    Aggregates all vessel activity within a time window.
    Returns one summary row per vessel: ping count, first/last seen, speed stats.

    Useful for: shift reports, traffic volume over time, idle vessel detection.
    """
    rows = conn.execute("""
        SELECT
            mmsi,
            name,
            COUNT(*)        AS ping_count,
            MIN(ts)         AS first_seen,
            MAX(ts)         AS last_seen,
            AVG(sog)        AS avg_speed_knots,
            MAX(sog)        AS max_speed_knots
        FROM ais
        WHERE ts BETWEEN ?::TIMESTAMP AND ?::TIMESTAMP
        GROUP BY mmsi, name
        ORDER BY ping_count DESC
    """, [window.start, window.end]).fetchall()

    columns = ["mmsi", "name", "ping_count", "first_seen", "last_seen",
               "avg_speed_knots", "max_speed_knots"]
    return [VesselSummary(**dict(zip(columns, row))) for row in rows]


def vessels_in_zone_during_window(
    conn: duckdb.DuckDBPyConnection,
    params: ZoneIntrusionParams,
) -> list[VesselPing]:
    """
    SPATIO-TEMPORAL query.

    Returns every ping inside a geographic zone AND within a time window.
    Optionally filtered by navigational status.

    Useful for: fishing violations in protected waters, night intrusion detection,
    incident timeline reconstruction.
    """
    base_sql = """
        SELECT
            mmsi, name, ship_type, nav_status,
            lat, lon, sog, ts AS timestamp
        FROM ais
        WHERE lat BETWEEN ? AND ?
          AND lon BETWEEN ? AND ?
          AND ts  BETWEEN ?::TIMESTAMP AND ?::TIMESTAMP
        {nav_filter_clause}
        ORDER BY ts ASC
    """

    if params.nav_status_filter:
        sql = base_sql.format(nav_filter_clause="AND nav_status = ?")
        query_params = [
            params.bbox.lat_min, params.bbox.lat_max,
            params.bbox.lon_min, params.bbox.lon_max,
            params.window.start, params.window.end,
            params.nav_status_filter,
        ]
    else:
        sql = base_sql.format(nav_filter_clause="")
        query_params = [
            params.bbox.lat_min, params.bbox.lat_max,
            params.bbox.lon_min, params.bbox.lon_max,
            params.window.start, params.window.end,
        ]

    rows = conn.execute(sql, query_params).fetchall()

    columns = ["mmsi", "name", "ship_type", "nav_status", "lat", "lon", "sog", "timestamp"]
    return [VesselPing(**dict(zip(columns, row))) for row in rows]