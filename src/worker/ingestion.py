"""
Reading and Inserting AIS data from a source into a DuckDB table.
Table col names are renamed to be cleaner.
"""

import duckdb

_CREATE_TABLE_SQL = """
CREATE OR REPLACE TABLE ais AS
SELECT
    TRY_CAST("# Timestamp" AS TIMESTAMP)  AS ts,
    "Type of mobile"                               AS mobile_type,
    TRY_CAST(MMSI AS BIGINT)                       AS mmsi,
    TRY_CAST(Latitude AS DOUBLE)                   AS lat,
    TRY_CAST(Longitude AS DOUBLE)                  AS lon,
    "Navigational status"                          AS nav_status,
    TRY_CAST(SOG AS DOUBLE)                        AS sog,
    TRY_CAST(COG AS DOUBLE)                        AS cog,
    TRY_CAST(Heading AS DOUBLE)                    AS heading,
    Name                                           AS name,
    "Ship type"                                    AS ship_type,
    Destination                                    AS destination
FROM read_csv_auto(
    ?,
    header    = true,
    nullstr   = ['Unknown', 'None', ''],
    ignore_errors = true
)
-- AIS default for "no data" is 0.0 — these are not real positions
WHERE TRY_CAST(Latitude  AS DOUBLE) != 0.0
  AND TRY_CAST(Longitude AS DOUBLE) != 0.0
  AND TRY_CAST("# Timestamp" AS TIMESTAMP) IS NOT NULL
"""


def ingest_csv(conn: duckdb.DuckDBPyConnection, path: str) -> int:
    """
    ETL: Load AIS records from csv into the db table.
    """
    try:
        conn.execute(_CREATE_TABLE_SQL, [path])
    except duckdb.IOException as e:
        raise RuntimeError(f"Could not read CSV at '{path}': {e}") from e

    row_count: int = conn.execute("SELECT COUNT(*) FROM ais").fetchone()[0]

    if row_count == 0:
        raise RuntimeError(f"CSV loaded but produced 0 valid rows: '{path}'")

    return row_count