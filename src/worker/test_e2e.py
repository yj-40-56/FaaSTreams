import json
import subprocess

WORKER_DIR = "/home/salam_zahr10/FaaSTreams/src/worker"

queries = [
    ("vessel_count", "SELECT COUNT(DISTINCT MMSI) as vessel_count FROM events"),
    ("stationary_vessels", "SELECT COUNT(*) as stationary_count FROM events WHERE NavigationalStatus IN ('At anchor', 'Moored')"),
    ("avg_speed_by_type", "SELECT shipType, ROUND(AVG(SOG), 2) as avg_speed FROM events WHERE SOG IS NOT NULL GROUP BY shipType ORDER BY avg_speed DESC"),
    ("wind_exclusion_zone", "SELECT Name, MMSI, Latitude, Longitude, SOG FROM events WHERE Latitude BETWEEN 55.5 AND 56.0 AND Longitude BETWEEN 7.5 AND 8.0 AND SOG IS NOT NULL AND SOG > 0.5"),
    ("kattegat_knn", "SELECT Name, MMSI, Latitude, Longitude, ROUND(SQRT(POWER(Latitude - 57.25, 2) + POWER(Longitude - 10.25, 2)), 4) as approx_distance FROM events WHERE Latitude IS NOT NULL ORDER BY approx_distance LIMIT 5"),
]

for name, query in queries:
    payload = json.dumps({
        "window_start": 0,
        "window_end": 9999999999,
        "query": query
    })
    result = subprocess.run(
        ["python3", "handler.py", payload],
        capture_output=True, text=True,
        cwd=WORKER_DIR
    )
    print(f"\n=== {name} ===")
    print(result.stdout[-2000:])
    if result.stderr:
        print("STDERR:", result.stderr[-500:])