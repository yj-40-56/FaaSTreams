import json
import sys
import functions_framework
import analytics
import fetch


@functions_framework.http
def handler(request):
    body = request.get_json(silent=True) or {}
    try:
        window_start = int(body["window_start"])
        window_end = int(body["window_end"])
        query = str(body["query"])
    except (KeyError, TypeError, ValueError) as e:
        return {"error": f"Invalid payload: {e}"}, 400

    query = body.get("query", "")
    if not query:
        return {"error": "Missing query in request body"}, 400

    print(f"Fetching window {window_start} - {window_end} from Redis...", flush=True)
    records = fetch.fetch_window(window_start, window_end)
    print(f"Loaded {len(records)} records.", flush=True)

    results = analytics.run(records, query)

    if check_warnings(results):
        print(f"\n*** {len(results)} PROXIMITY WARNING(S) ***\n", flush=True)
        for w in results:
            print(
                f"  VESSEL {w['mmsi']} ({w['name']}) — {w['distance_nm']} nm from \"{w['zone_name']}\""
                f" (threshold {w['threshold_nm']} nm)"
                f" | sog={w['sog']} kn | status={w['navigationalStatus']} | ts={w['timestamp']}",
                flush=True
            )
    else:
        print("No proximity warnings.", flush=True)

    fetch.delete_window(window_start, window_end)

    return {"results": results, "records_processed": len(records)}


# TODO: Fill this out
def check_warnings(results):
    return False


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
