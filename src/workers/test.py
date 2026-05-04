from multiprocessing import connection

import duckdb
import functions_framework
import redis
import os
import pandas as pd

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

@functions_framework.http
def duckdb_request(request):
    connection = duckdb.connect()
    try:
        connection.execute("SELECT 1;")
        return "DuckDB test passed", 200
    except Exception as e:   
        return f"DuckDB test failed: {e}", 500
    finally:
        connection.close()


@functions_framework.http
def duckdb_ais_test(request):
    connection = duckdb.connect()
    try:
        connection.execute("INSTALL spatial; LOAD spatial")
        csv_path = "../../data/ais.csv"
        query = f"""
            SELECT count(DISTINCT MMSI) 
            FROM read_csv_auto('{csv_path}')
            WHERE Latitude > 50.0 AND Longitude > 10.0 AND "Navigational Status" = 'Under way using engine'
        """
        result = connection.execute(query).fetchone()[0]
        query = f"""
            SELECT MMSI, 
                   min("# Timestamp") as arrival, 
                   max("# Timestamp") as departure,
                   (max("# Timestamp") - min("# Timestamp")) as duration
            FROM read_csv_auto('{csv_path}')
            GROUP BY MMSI
            HAVING duration > INTERVAL '1 hour';
        """
        result_long_stay = connection.execute(query).fetchall()
        return {
            "status": "success",
            "ships_in_zone": result,
            "ships_with_long_stays": result_long_stay,
            "engine": "DuckDB + Spatial"
        }, 200
    
    except Exception as e:   
        return f"Error: {str(e)}", 500
    finally:
        connection.close()

@functions_framework.http
def ais_redis_test(request):
    connection = duckdb.connect()
    try:
        # 1. Fetch total count and data from Redis
        total_records = int(r.get("ais:total") or 0)
        
        ais_data = []
        for i in range(min(total_records, 1000)):
            record = r.hgetall(f"ais:{i}")
            if record:
                ais_data.append(record)

        if not ais_data:
            return {"status": "error", "message": "No data found in Redis"}, 404

        df = pd.DataFrame(ais_data)
        connection.execute("INSTALL spatial; LOAD spatial")
        

        # basic
        basic_query = """
            SELECT count(DISTINCT mmsi) 
            FROM df
            """
        basic_count = connection.execute(basic_query).fetchone()[0]
        

        # spatial
        spatial_query = """
            SELECT count(DISTINCT mmsi) 
            FROM df
            WHERE CAST(latitude AS FLOAT) > 50.0 
              AND CAST(longitude AS FLOAT) > 10.0 
              AND navigationalStatus = 'Under way using engine'
        """
        ships_in_zone = connection.execute(spatial_query).fetchone()[0]

        # temporal
        temporal_query = """
            WITH parsed AS (
                SELECT mmsi, strptime(timestamp, '%d/%m/%Y %H:%M:%S') as dt FROM df
            )
            SELECT 
                mmsi, 
                min(dt) as first_seen, 
                max(dt) as last_seen
            FROM parsed
            GROUP BY mmsi
        """
        first_last_seen = connection.execute(temporal_query).df().to_dict(orient='records')

        return {
            "status": "success",
            "source": "Redis",
            "records_processed": len(ais_data),
            "ships_in_area": ships_in_zone,
            "ship_first_last_seen": first_last_seen,
            "basic_count": basic_count
        }, 200

    except Exception as e:
        return {"status": "error", "message": str(e)}, 500
    finally:
        connection.close()