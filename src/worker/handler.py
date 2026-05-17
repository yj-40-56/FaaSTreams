import json
import sys

import functions_framework

import analytics
import fetch


@functions_framework.http
def handler(request):
    body = request.get_json(silent=True) or {}
    try:
        start_index = int(body["start_index"])
        end_index = int(body["end_index"])
    except (KeyError, TypeError, ValueError) as e:
        return {"error": f"Invalid payload: {e}"}, 400

    print(f"Fetching records {start_index}–{end_index} from Redis...")
    records = fetch.fetch_range(start_index, end_index)
    print(f"Loaded {len(records)} records.")

    warnings = analytics.run(records)

    if warnings:
        print(f"\n*** {len(warnings)} PROXIMITY WARNING(S) ***\n")
        for w in warnings:
            print(
                f"  VESSEL {w['mmsi']} ({w['name']}) — {w['distance_nm']} nm from \"{w['zone_name']}\""
                f" (threshold {w['threshold_nm']} nm)"
                f" | sog={w['sog']} kn | status={w['navigationalStatus']} | ts={w['timestamp']}"
            )
    else:
        print("No proximity warnings.")

    return {"warnings": warnings, "records_processed": len(records)}


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
