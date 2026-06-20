"""
Data flow:
Build a DuckDB table from the window's records, 
optionally attach a zones reference table, 
run the given SQL,
return rows. 

This is generalized MOD schema
"""

import duckdb

EVENTS_SCHEMA = """
    CREATE TABLE events (
        object_id    VARCHAR,
        lat          DOUBLE,
        lon          DOUBLE,
        ts           VARCHAR,
        speed        DOUBLE,
        heading      DOUBLE,
        object_type  VARCHAR,
        status       VARCHAR
    )
"""

ZONES_SCHEMA = """
    CREATE TABLE zones (
        zone_name    VARCHAR,
        geom_wkt     VARCHAR,
        threshold_nm DOUBLE
    )
"""


def _load_spatial_extension(conn: duckdb.DuckDBPyConnection) -> None:
    try:
        # Cloud Functions/Cloud Run only allow writes under /tmp -- without
        # this, INSTALL fails in deployment even though it can look fine
        # when run locally with a normal home directory.
        conn.execute("SET extension_directory='/tmp'")
        conn.execute("INSTALL spatial")
        conn.execute("LOAD spatial")
    except Exception as exc:
        print(f"Warning: could not load spatial extension: {exc}", flush=True)


def run(records: list[dict], query: str, zones: list[dict] | None = None) -> list[dict]:
    """
    Input:  records - canonical-schema dicts (already normalized by
                       normalize.py), e.g. {"object_id": "123", "lat": 55.1, ...}
            query    - SQL string to execute against the `events` table
                       (and `zones`, if `zones` is provided and the query
                       needs it)
            zones    - optional reference rows for spatial queries that
                       JOIN against a `zones` table
    Output: list of result rows as dicts. Returns [] (without running the
            query) if there are no records with both lat and lon.
    """
    valid_records = [r for r in records if r.get("lat") is not None and r.get("lon") is not None]
    if not valid_records:
        print("No valid records with coordinates", flush=True)
        return []

    conn = duckdb.connect()
    try:
        _load_spatial_extension(conn)

        conn.execute(EVENTS_SCHEMA)
        conn.executemany(
            "INSERT INTO events VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    r.get("object_id"),
                    float(r["lat"]),
                    float(r["lon"]),
                    r.get("ts"),
                    float(r["speed"]) if r.get("speed") is not None else None,
                    float(r["heading"]) if r.get("heading") is not None else None,
                    r.get("object_type"),
                    r.get("status"),
                )
                for r in valid_records
            ],
        )

        if zones:
            conn.execute(ZONES_SCHEMA)
            conn.executemany(
                "INSERT INTO zones VALUES (?, ?, ?)",
                [(z["zone_name"], z["geom_wkt"], z["threshold_nm"]) for z in zones],
            )

        print(f"Executing query: {query}", flush=True)
        cursor = conn.execute(query)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    except Exception as exc:
        print(f"Error executing query: {exc}", flush=True)
        raise
    finally:
        conn.close()