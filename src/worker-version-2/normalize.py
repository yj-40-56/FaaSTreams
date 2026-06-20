"""
Renames each raw record's domain-specific fields (MMSI, Latitude, ...) into a small
canonical schema, so analytics.py and the SQL you write never have to
know which MOD domain's data is actually flowing through.

  - normalize_record / normalize_records: pure field renaming. Unchanged
    from before.

  - validate_record / validate_records: checks the renamed record is
    actually usable, and parses `ts` into a canonical
    ISO 8601 string for duckdb computations
   
"""

from datetime import datetime, timezone

CANONICAL_FIELDS = [
    "object_id",
    "lat",
    "lon",
    "ts",
    "speed",
    "heading",
    "object_type",
    "status",
]

# A record needs usable values for these to be worth keeping -- every
# query written so far groups/partitions by object_id and needs a real
# position and timestamp. speed/heading/object_type/status stay optional,
# exactly as the canonical schema always intended.
REQUIRED_FIELDS = ["object_id", "lat", "lon", "ts"]

# Above this, a numeric timestamp is almost certainly milliseconds, not
# seconds (seconds-since-epoch doesn't reach this value until the year 2286).
_EPOCH_MS_THRESHOLD = 10_000_000_000


def normalize_record(record: dict, mapping: dict) -> dict:
    """
    Input:  record  - one raw dict as decoded from Redis,
                       e.g. {"MMSI": "123", "Latitude": 55.1, ...}
            mapping - {canonical_field: source_field}, from domain.yml,
                       e.g. {"object_id": "MMSI", "lat": "Latitude", ...}
    Output: a dict with canonical keys. A canonical field with no source
            in `mapping`, or missing from this particular record, comes
            through as None rather than raising.
    """
    return {
        canonical_field: record.get(source_field)
        for canonical_field, source_field in mapping.items()
    }


def normalize_records(records: list[dict], mapping: dict) -> list[dict]:
    return [normalize_record(r, mapping) for r in records]


def _parse_timestamp(raw_ts, ts_format: str | None) -> tuple[str | None, str | None]:
    """
    Parse a raw timestamp value into a canonical ISO 8601 string.

    Tries, in order:
      1. Numeric epoch (seconds or milliseconds) -- if `raw_ts` is a number.
      2. ISO 8601 -- the canonical target format. If a domain already
         sends ISO 8601, this always succeeds and `ts_format` is never needed.
      3. `ts_format` -- a domain-specific strptime format (e.g.
         "%d/%m/%Y %H:%M:%S" for the raw AIS field), if given.

    A successfully parsed but timezone-naive result is assumed UTC --
    true for AIS by maritime convention, but confirm this for any other
    domain before relying on it.

    Returns (iso_string, None) on success, or (None, reason) on failure.
    """
    if raw_ts is None:
        return None, "missing timestamp"

    if isinstance(raw_ts, (int, float)) and not isinstance(raw_ts, bool):
        try:
            seconds = raw_ts / 1000 if raw_ts > _EPOCH_MS_THRESHOLD else raw_ts
            parsed = datetime.fromtimestamp(seconds, tz=timezone.utc)
            return parsed.isoformat().replace("+00:00", "Z"), None
        except (OverflowError, OSError, ValueError):
            return None, f"invalid epoch timestamp: {raw_ts!r}"

    if not isinstance(raw_ts, str):
        return None, f"unsupported timestamp type: {type(raw_ts).__name__}"

    parsed = None
    try:
        parsed = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
    except ValueError:
        pass

    if parsed is None and ts_format:
        try:
            parsed = datetime.strptime(raw_ts, ts_format)
        except ValueError:
            pass

    if parsed is None:
        return None, f"unparseable timestamp: {raw_ts!r}"

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.isoformat().replace("+00:00", "Z"), None


def _validate_coordinate(value, min_value: float, max_value: float, label: str) -> tuple[float | None, str | None]:
    if value is None:
        return None, f"missing {label}"
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None, f"{label} is not numeric: {value!r}"
    if not (min_value <= value <= max_value):
        return None, f"{label} out of range: {value}"
    return value, None


def validate_record(record: dict, ts_format: str | None = None) -> tuple[dict | None, str | None]:
    """
    Input:  a canonical-schema record (already through normalize_record).
            ts_format - optional strptime format for this domain's raw
            timestamp string, e.g. "%d/%m/%Y %H:%M:%S". Set per-domain in
            domain.yml; leave unset if the domain already sends ISO 8601.
    Output: (validated_record, None) if the record is usable, or
            (None, reason) if it should be dropped. On success, `ts` in
            the returned record is always a canonical ISO 8601 string and
            `lat`/`lon` are floats, regardless of what types/format they
            arrived as.
    """
    object_id = record.get("object_id")
    if object_id is None or str(object_id).strip() == "":
        return None, "missing object_id"

    lat, lat_error = _validate_coordinate(record.get("lat"), -90.0, 90.0, "lat")
    if lat_error:
        return None, lat_error

    lon, lon_error = _validate_coordinate(record.get("lon"), -180.0, 180.0, "lon")
    if lon_error:
        return None, lon_error

    ts, ts_error = _parse_timestamp(record.get("ts"), ts_format)
    if ts_error:
        return None, ts_error

    return {**record, "object_id": str(object_id), "lat": lat, "lon": lon, "ts": ts}, None


def validate_records(
    records: list[dict], ts_format: str | None = None
) -> tuple[list[dict], list[tuple[dict, str]]]:
    """Returns (valid_records, dropped) where dropped is [(original_record, reason), ...]."""
    valid, dropped = [], []
    for record in records:
        validated, reason = validate_record(record, ts_format=ts_format)
        if validated is not None:
            valid.append(validated)
        else:
            dropped.append((record, reason))
    return valid, dropped


def normalize_and_validate(
    records: list[dict], mapping: dict, ts_format: str | None = None
) -> tuple[list[dict], list[tuple[dict, str]]]:
    """Convenience wrapper: rename then validate in one call."""
    normalized = normalize_records(records, mapping)
    return validate_records(normalized, ts_format=ts_format)