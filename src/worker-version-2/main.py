"""
main.py

Same flow as your original handler: parse payload -> fetch window -> run
query -> forward to sink -> return results. Two changes:

1. A normalize step between fetch and run, so analytics.py only ever sees
   canonical field names regardless of which MOD domain's data this
   deployment is fed.
2. `process()` takes its IO functions (fetch_fn, run_fn, sink_fn) as
   parameters with the real modules as defaults. In production nothing
   changes -- it behaves exactly like calling fetch.fetch_window /
   analytics.run / sink.forward directly. In tests, you pass fakes and get
   full orchestration coverage with no real Redis or HTTP involved, while
   analytics.run still executes real SQL against real DuckDB.
"""

import json
import sys

import functions_framework

import analytics
import domain
import fetch
import normalize
import sink

DOMAIN_MAPPING = domain.load_mapping()
TS_FORMAT = domain.load_ts_format()
ZONES = domain.load_zones()


def process(
    window_start: int,
    window_end: int,
    query: str,
    query_name: str,
    return_type: str,
    fetch_fn=fetch.fetch_window,
    run_fn=analytics.run,
    sink_fn=sink.forward,
) -> dict:
    print(f"Fetching window {window_start} - {window_end} from Redis...", flush=True)
    raw_records = fetch_fn(window_start, window_end)
    print(f"Loaded {len(raw_records)} raw record(s).", flush=True)

    records, dropped = normalize.normalize_and_validate(raw_records, DOMAIN_MAPPING, ts_format=TS_FORMAT)
    if dropped:
        reasons = [reason for _, reason in dropped[:5]]
        more = f" (+{len(dropped) - 5} more)" if len(dropped) > 5 else ""
        print(f"Dropped {len(dropped)} invalid record(s): {reasons}{more}", flush=True)

    results = run_fn(records, query, zones=ZONES)
    print(f"Query produced {len(results)} result(s).", flush=True)

    sink_fn(results, window_start, window_end, query, query_name, return_type)

    return {
        "results": results,
        "records_processed": len(records),
        "records_dropped": len(dropped),
        "query_name": query_name,
        "return_type": return_type,
    }


def parse_payload(body: dict) -> tuple[int, int, str, str, str]:
    try:
        window_start = int(body["window_start"])
        window_end = int(body["window_end"])
        query = str(body["query"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"Invalid payload: {exc}") from exc

    query_name = body.get("query_name", "unknown")
    return_type = body.get("return_type", "unknown")
    return window_start, window_end, query, query_name, return_type


@functions_framework.http
def handler(request):
    body = request.get_json(silent=True) or {}

    try:
        window_start, window_end, query, query_name, return_type = parse_payload(body)
    except ValueError as exc:
        return {"error": str(exc)}, 400

    result = process(window_start, window_end, query, query_name, return_type)
    return result, 200


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