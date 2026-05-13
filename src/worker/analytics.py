import duckdb

from zones import HAZARD_ZONES

METERS_PER_NM = 1852.0


def run(records: list[dict]) -> list[dict]:
    if not records:
        return []

    conn = duckdb.connect()
    conn.execute("INSTALL spatial")
    conn.execute("LOAD spatial")

    conn.execute("""
        CREATE TABLE vessels (
            mmsi        VARCHAR,
            name        VARCHAR,
            latitude    DOUBLE,
            longitude   DOUBLE,
            sog         DOUBLE,
            timestamp   VARCHAR,
            navigationalStatus VARCHAR
        )
    """)

    conn.executemany(
        "INSERT INTO vessels VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (
                r.get("mmsi", ""),
                r.get("name", ""),
                float(r["latitude"]) if r.get("latitude") else None,
                float(r["longitude"]) if r.get("longitude") else None,
                float(r["sog"]) if r.get("sog") else None,
                r.get("timestamp", ""),
                r.get("navigationalStatus", ""),
            )
            for r in records
            if r.get("latitude") and r.get("longitude")
        ],
    )

    conn.execute("""
        CREATE TABLE zones (
            zone_name    VARCHAR,
            geom_wkt     VARCHAR,
            threshold_nm DOUBLE
        )
    """)

    conn.executemany(
        "INSERT INTO zones VALUES (?, ?, ?)",
        [(z["name"], z["wkt"], z["threshold_nm"]) for z in HAZARD_ZONES],
    )

    warnings = conn.execute("""
        SELECT
            v.mmsi,
            v.name,
            v.timestamp,
            v.sog,
            v.navigationalStatus,
            z.zone_name,
            z.threshold_nm,
            ROUND(
                ST_Distance(
                    ST_Transform(ST_Point(v.longitude, v.latitude), 'EPSG:4326', 'EPSG:3857'),
                    ST_Transform(ST_GeomFromText(z.geom_wkt),       'EPSG:4326', 'EPSG:3857')
                ) / ?
            , 2) AS distance_nm
        FROM vessels v
        CROSS JOIN zones z
        WHERE v.latitude IS NOT NULL
          AND ST_Distance(
                ST_Transform(ST_Point(v.longitude, v.latitude), 'EPSG:4326', 'EPSG:3857'),
                ST_Transform(ST_GeomFromText(z.geom_wkt),       'EPSG:4326', 'EPSG:3857')
              ) / ? < z.threshold_nm
        ORDER BY distance_nm
    """, [METERS_PER_NM, METERS_PER_NM]).fetchall()

    columns = ["mmsi", "name", "timestamp", "sog", "navigationalStatus", "zone_name", "threshold_nm", "distance_nm"]
    return [dict(zip(columns, row)) for row in warnings]
