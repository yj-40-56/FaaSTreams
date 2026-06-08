import duckdb

from zones import HAZARD_ZONES

def run(records: list[dict], query: str) -> list[dict]:
    if not records:
        return []

    _conn = duckdb.connect()
    _conn.execute("INSTALL spatial")
    _conn.execute("LOAD spatial")

    _conn.execute("DROP TABLE IF EXISTS vessels")
    _conn.execute("""
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

    _conn.executemany(
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

    _conn.execute("DROP TABLE IF EXISTS zones")
    _conn.execute("""
        CREATE TABLE zones (
            zone_name    VARCHAR,
            geom_wkt     VARCHAR,
            threshold_nm DOUBLE
        )
    """)

    _conn.executemany(
        "INSERT INTO zones VALUES (?, ?, ?)",
        [(z["name"], z["wkt"], z["threshold_nm"]) for z in HAZARD_ZONES],
    )

    cursor = _conn.execute(query)
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]
