import duckdb
from zones import HAZARD_ZONES


def run(records: list[dict], query: str) -> list[dict]:
    if not records:
        return []

    conn = duckdb.connect()

    try:
        # Load spatial extension for geographic operations
        conn.execute("LOAD spatial")
    except Exception as e:
        print(f"Warning: Could not load spatial extension: {e}", flush=True)

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

    try:
        print(f"Executing query: {query}", flush=True)
        cursor = conn.execute(query)
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    except Exception as e:
        print(f"Error executing query: {e}", flush=True)
        raise
    finally:
        conn.close()

    return results