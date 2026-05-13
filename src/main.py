"""
GCP Cloud Function entrypoint.
Manages parsing the HTTP request, call the right query, return JSON.
"""

import json
import logging
import os
from datetime import datetime
from pathlib import Path

import functions_framework

from worker import (
    create_connection,
    ingest_csv,
    vessels_in_bbox,
    vessel_activity_in_window,
    vessels_in_zone_during_window,
    BoundingBox,
    TimeWindow,
    ZoneIntrusionParams,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# for now static csv data, later GCS path
CSV_PATH = os.getenv(
    "AIS_CSV_PATH",
    str(Path(__file__).parent.parent / "data" / "ais.csv")
)

def _serialize(obj):
    """JSON serializer for types not handled by default (e.g. datetime)."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


@functions_framework.http
def ais_worker(request):
    """
    HTTP-triggered Cloud Function.

    Expected JSON body:
        { "query": "spatial" | "temporal" | "spatio_temporal", ...params }

    Returns:
        200 + list of result objects on success
        400 on bad input
        500 on internal error
    """
    body = request.get_json(silent=True) or {}
    query_type = body.get("query")

    if not query_type:
        return {"error": "Missing required field: 'query'"}, 400

    logger.info(f"Received query type: {query_type}")

    try:
        conn = create_connection()
        row_count = ingest_csv(conn, CSV_PATH)
        logger.info(f"Ingested {row_count} rows from {CSV_PATH}")

        if query_type == "spatial":
            bbox = BoundingBox(**{k: body[k] for k in ["lat_min", "lat_max", "lon_min", "lon_max"]})
            results = vessels_in_bbox(conn, bbox)

        elif query_type == "temporal":
            window = TimeWindow(start=body["start"], end=body["end"])
            results = vessel_activity_in_window(conn, window)

        elif query_type == "spatio_temporal":
            params = ZoneIntrusionParams(
                bbox=BoundingBox(**{k: body[k] for k in ["lat_min", "lat_max", "lon_min", "lon_max"]}),
                window=TimeWindow(start=body["start"], end=body["end"]),
                nav_status_filter=body.get("nav_status_filter"),
            )
            results = vessels_in_zone_during_window(conn, params)

        else:
            return {"error": f"Unknown query type: '{query_type}'"}, 400

    except KeyError as e:
        return {"error": f"Missing required parameter: {e}"}, 400
    except ValueError as e:
        return {"error": f"Invalid parameter: {e}"}, 400
    except RuntimeError as e:
        logger.error(f"Worker error: {e}")
        return {"error": str(e)}, 500

    payload = [r.model_dump() for r in results]
    return json.loads(json.dumps(payload, default=_serialize)), 200