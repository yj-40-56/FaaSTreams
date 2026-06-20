"""
Renames each raw record's domain-specific fields (MMSI, Latitude, ...) into a small
canonical schema, so analytics.py and the SQL you write never have to
know which MOD domain's data is actually flowing through.

Pure functions, no I/O -- trivial to unit test with a literal dict.
"""

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