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
    request_json = request.get_json(silent=True)
    if request_json is None:
        request_json = {}
    start_idx = request_json.get('start', 0)
    end_idx = request_json.get('end', 100)
    analysis_type = "basic"
    if request_json and 'type' in request_json:
        analysis_type = request_json['type']

    connection = duckdb.connect()
    try:
        ais_data = []
        for i in range(start_idx, end_idx):
            record = r.hgetall(f"ais:{i}")
            if record:
                ais_data.append(record)

        if not ais_data:
            return {"status": "error", "message": "No data found for this window"}, 404 

        df = pd.DataFrame(ais_data)
        connection.execute("INSTALL spatial; LOAD spatial")

        if analysis_type == "basic":        
            basic_query = """
            SELECT count(DISTINCT mmsi) 
            FROM df
            """
            result = connection.execute(basic_query).fetchone()[0]
        

        # spatial
        elif analysis_type == "spatial":   
            spatial_query = """
                SELECT count(DISTINCT mmsi) 
                FROM df
                WHERE CAST(latitude AS FLOAT) > 50.0 
                AND CAST(longitude AS FLOAT) > 10.0 
                AND navigationalStatus = 'Under way using engine'
            """
            result = connection.execute(spatial_query).fetchone()[0]

        # temporal
        elif analysis_type == "temporal":   
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
            result = connection.execute(temporal_query).df().to_dict(orient='records')
        else:
            return {"status": "error", "message": f"Unknown analysis type: {analysis_type}"}, 400

        return {
            "status": "success",
            "source": "Redis",
            "records_processed": len(ais_data),
            "analysis_executed": analysis_type,
            "data": result
        }, 200

    except Exception as e:
        return {"status": "error", "message": str(e)}, 500
    finally:
        connection.close()

'''
how to run locally:
1. Docker Desktop running with Redis container and imported AIS data
2. Start worker using: `functions-framework --target=ais_redis_test`
    - This will start the HTTP server on port 8080 by default.
3. In second terminal run (for example, testing spatial analysis on first 50 records):
    curl.exe -X POST http://localhost:8080 `
     -H "Content-Type: application/json" `
     -d '{\"type\": \"spatial\", \"start\": 0, \"end\": 50}'
'''