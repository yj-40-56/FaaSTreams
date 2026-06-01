import duckdb


def run(records: list[dict], query: str) -> list[dict]:
    valid_records = [r for r in records if r.get("Latitude") and r.get("Longitude")]
    if not valid_records:
        print("No valid records with coordinates", flush=True)
        return []
_conn = duckdb.connect()
_conn.execute("INSTALL spatial")
_conn.execute("LOAD spatial")


def run(records: list[dict], query: str) -> list[dict]:
    if not records:
        return []

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

    conn.executemany(
        "INSERT INTO events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
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
