import duckdb

from zones import HAZARD_ZONES

METERS_PER_NM = 1852.0

# TODO: Use query sent by coordinator instead of hardcoding
def run(records: list[dict]) -> list[dict]:
    valid_records = [r for r in records if r.get("Latitude") and r.get("Longitude")]
    if not valid_records:
        print("No valid records with coordinates", flush=True)
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
                r.get("MMSI", ""),
                r.get("Name", ""),
                float(r["Latitude"]) if r.get("Latitude") else None,
                float(r["Longitude"]) if r.get("Longitude") else None,
                float(r["SOG"]) if r.get("SOG") else None,
                r.get("# Timestamp", ""),
                r.get("Navigational status", ""),
            )
            for r in records
            if r.get("Latitude") and r.get("Longitude")
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
