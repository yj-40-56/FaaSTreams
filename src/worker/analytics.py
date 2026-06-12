import duckdb
from zones import HAZARD_ZONES

def run(records: list[dict], query: str) -> list[dict]:
    valid_records = [r for r in records if r.get("Latitude") and r.get("Longitude")]
    if not valid_records:
        print("No valid records with coordinates", flush=True)
        return []

    conn = duckdb.connect()
    try:
        conn.execute("LOAD spatial")
    except Exception as e:
        print(f"Warning: Could not load spatial extension: {e}", flush=True)

    conn.execute("""
        CREATE TABLE events (
            MMSI                VARCHAR,
            Name                VARCHAR,
            Latitude            DOUBLE,
            Longitude           DOUBLE,
            SOG                 DOUBLE,
            Timestamp           VARCHAR,
            NavigationalStatus  VARCHAR,
            shipType            VARCHAR,
            typeOfMobile        VARCHAR,
            heading             DOUBLE,
            destination         VARCHAR
        )
    """)

    rows = [
        (
            r.get("MMSI", ""),
            r.get("Name", ""),
            float(r["Latitude"]),
            float(r["Longitude"]),
            float(r["SOG"]) if r.get("SOG") else None,
            r.get("# Timestamp", ""),
            r.get("Navigational status", ""),
            r.get("shipType", ""),
            r.get("typeOfMobile", ""),
            float(r["heading"]) if r.get("heading") else None,
            r.get("destination", ""),
        )
        for r in valid_records
    ]

    # Single multi-row INSERT instead of executemany: executemany prepares/executes
    # one statement per row, which is far too slow and memory-heavy for large windows.
    placeholders = ",".join(["(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"] * len(rows))
    flat_params = [value for row in rows for value in row]
    conn.execute(f"INSERT INTO events VALUES {placeholders}", flat_params)

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