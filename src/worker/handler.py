import json
import sys

import analytics
import fetch


def handler(event: dict, context) -> dict:
    start_index = int(event["start_index"])
    end_index = int(event["end_index"])

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
    event = json.loads(sys.argv[1])
    handler(event, None)
