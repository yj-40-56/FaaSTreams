"""
creates a stateless, in-memory connection to DuckDB, load spatial is for when we will use shapefile suppport
"""
 
import duckdb
 
 
def create_connection(load_spatial: bool = False) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(database=":memory:")
    if load_spatial:
        conn.execute("INSTALL spatial;")
        conn.execute("LOAD spatial;")
    return conn
