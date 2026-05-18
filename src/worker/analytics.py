import duckdb

from zones import HAZARD_ZONES

_conn = duckdb.connect()
_conn.execute("INSTALL spatial")
_conn.execute("LOAD spatial")


def run(records: list[dict], query: str) -> list[dict]:
    if not records:
        return []

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
