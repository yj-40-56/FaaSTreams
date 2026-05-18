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

    print(f"Fetching records {window_start}–{window_end} from Redis...")
    records = fetch.fetch_range(window_start, window_end)
    print(f"Loaded {len(records)} records.")

    results = analytics.run(records, query)
    print(f"{len(results)} result(s).")

    return {"results": results, "records_processed": len(records)}


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