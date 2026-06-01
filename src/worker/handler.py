import json
import os
import sys
import urllib.request
import urllib.error
import functions_framework
import analytics
import fetch

DATA_SINK_URL = os.getenv("DATA_SINK_URL")


@functions_framework.http
def handler(request):
    body = request.get_json(silent=True) or {}
    try:
        window_start = int(body["window_start"])
        window_end = int(body["window_end"])
        query = str(body["query"])
    except (KeyError, TypeError, ValueError) as e:
        return {"error": f"Invalid payload: {e}"}, 400

    print(f"Fetching records {window_start}–{window_end} from Redis...", flush=True)
    records = fetch.fetch_window(window_start, window_end)
    print(f"Loaded {len(records)} records.", flush=True)

    results = analytics.run(records, query)
    print(f"{len(results)} result(s).", flush=True)

    _forward_to_sink(results, window_start, window_end, query)

    return {"results": results, "records_processed": len(records)}


def _forward_to_sink(results, window_start, window_end, query):
    if not DATA_SINK_URL:
        print("DATA_SINK_URL not set, skipping data sink.", flush=True)
        return
    payload = json.dumps({
        "results": results,
        "window_start": window_start,
        "window_end": window_end,
        "query": query,
    }).encode()
    req = urllib.request.Request(DATA_SINK_URL, data=payload, headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, timeout=10)
        print("Forwarded results to data sink.", flush=True)
    except urllib.error.URLError as e:
        print(f"Failed to forward to data sink: {e}", flush=True)


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
