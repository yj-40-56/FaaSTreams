import json
import sys

import analytics
import fetch
import functions_framework

@functions_framework.http
def handler(request):
    request_json = request.get_json(silent=True)

    if request_json and "start_timestamp" in request_json:
        start = int(request_json["start_timestamp"])
        end = int(request_json["end_timestamp"])
        records = fetch.fetch_by_timestamp(start, end)
    else:
        return {"error": "Invalid input, Timestamps expected"}, 400
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

    return {"status": "success", "warnings": warnings, "records_processed": len(records)}, 200

# if __name__ == "__main__":
#     event = json.loads(sys.argv[1])
#     handler(event, None)
