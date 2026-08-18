import datetime
import json
import os
import sys
import time
import traceback
import urllib.request
import urllib.error
import functions_framework
import analytics
import fetch

DATA_SINK_URL = os.getenv("DATA_SINK_URL")

@functions_framework.http
def handler(request):
    worker_start = int(time.time() * 1000)

    body = request.get_json(silent=True) or {}
    try:
        window_start = int(body["window_start"])
        window_end   = int(body["window_end"])
        query_name   = str(body["query_name"])
        source = {
            "columns":          body["columns"],
            "reference_tables": body.get("reference_tables") or {},
        }
        query_config = {
            "query":        str(body["query"]),
            "return_type":  str(body["return_type"]),
            "is_alert":     bool(body.get("is_alert", False)),
            "alert_format": str(body.get("alert_format", "")),
            "data_source":  str(body["data_source"]),
        }
        ingestor_start = body.get("ingestor_start", 0)
        ingestor_end   = body.get("ingestor_end", 0)
        windower_start = body.get("windower_start", 0)
        windower_end   = body.get("windower_end", 0)
    except (KeyError, TypeError, ValueError) as e:
        print(f"[Worker] Invalid payload: {e} | body={body}", flush=True)
        return {"error": f"Invalid payload: {e}"}, 400

    if query_config["data_source"] == "generic":
            query_config["query"] = normalize_query(query_config["query"], body)

    log_prefix = f"[Worker:{query_name}]"

    start_human = time.strftime("%H:%M:%S", time.gmtime(window_start))
    end_human = time.strftime("%H:%M:%S", time.gmtime(window_end))
    print(f"{log_prefix} Received window {start_human} - {end_human} ({window_start}-{window_end})", flush=True)

    try:
        print(f"{log_prefix} Fetching window from Redis...", flush=True)
        records = fetch.fetch_window(window_start, window_end, query_config["data_source"])
        print(f"{log_prefix} Loaded {len(records)} records.", flush=True)
        results = analytics.run(records, query_config["query"], source)
        print(f"{log_prefix} {len(results)} result(s): {results}", flush=True)
    except Exception as e:
        print(f"{log_prefix} ERROR while processing window: {e}\n{traceback.format_exc()}", flush=True)
        return {"error": str(e)}, 500
    
    worker_end = int(time.time() * 1000)
    timestamps = {
        "ingestor_start": ingestor_start,  # t0
        "ingestor_end":   ingestor_end,    # t1
        "windower_start": windower_start,  # t2
        "windower_end":   windower_end,    # t3
        "worker_start":   worker_start,    # t4
        "worker_end":     worker_end,      # t5
    }
    if query_config["is_alert"] and results:
        fmt = query_config["alert_format"]
        print(f"\n*** {len(results)} {query_name.upper()} WARNING(S) ***\n", flush=True)
        for w in results:
            try:
                print(f"  {fmt.format(**w)}", flush=True)
            except KeyError as e:
                print(f"  {w}  (format error: missing field {e})", flush=True)
    else:
        print(f"{log_prefix} No alerts ({len(results)} result(s)).", flush=True)

    # fetch.delete_window(window_start, window_end)
    _forward_to_sink(results, window_start, window_end, query_name, query_config["return_type"], timestamps)

    print(f"{log_prefix} Done.", flush=True)

    return {
        "results": results,
        "records_processed": len(records),
        "query_name": query_name,
        "return_type": query_config["return_type"]
    }

def _forward_to_sink(results, window_start, window_end, query_name, return_type, timestamps):
    log_prefix = f"[Worker:{query_name}]"
    if not DATA_SINK_URL:
        print(f"{log_prefix} DATA_SINK_URL not set, skipping data sink.", flush=True)
        return
    payload = json.dumps({
        "results": results,
        "window_start": window_start,
        "window_end": window_end,
        "query_name": query_name,
        "return_type": return_type,
        "ingestor_start": timestamps["ingestor_start"],
        "ingestor_end":   timestamps["ingestor_end"],
        "windower_start": timestamps["windower_start"],
        "windower_end":   timestamps["windower_end"],
        "worker_start":   timestamps["worker_start"],
        "worker_end":     timestamps["worker_end"],
    }).encode()
    req = urllib.request.Request(DATA_SINK_URL, data=payload, headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, timeout=10)
        print(f"{log_prefix} Forwarded results to data sink.", flush=True)
    except urllib.error.URLError as e:
        print(f"{log_prefix} Failed to forward to data sink: {e}", flush=True)

def normalize_query(query: str, body: dict) -> str:
    return query.format_map({
        "id_field":        body["id_field"],
        "timestamp_field": body["timestamp_field"],
        "lat_field":       body["lat_field"],
        "lon_field":       body["lon_field"],
    })

if __name__ == "__main__":
    import flask
    app = flask.Flask(__name__)
    with app.test_request_context(
        method="POST",
        json=json.loads(sys.argv[1]),
        content_type="application/json",
    ):
        result = handler(flask.request)
    print(json.dumps(result if isinstance(result, dict) else result[0], indent=2))