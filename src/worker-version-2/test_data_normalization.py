import normalize

MAPPING = {
    "object_id": "MMSI",
    "lat": "Latitude",
    "lon": "Longitude",
    "ts": "# Timestamp",
    "speed": "SOG",
    "object_type": "shipType",
    "status": "Navigational status",
}


# ---------------------------------------------------------------------------
# normalize_record / normalize_records -- unchanged behavior
# ---------------------------------------------------------------------------

def test_normalize_record_renames_fields():
    raw = {
        "MMSI": "123456", "Latitude": 55.1, "Longitude": 7.2,
        "# Timestamp": "2026-06-20T12:00:00Z", "SOG": 12.3,
        "shipType": "cargo", "Navigational status": "Under way",
    }
    result = normalize.normalize_record(raw, MAPPING)
    assert result["object_id"] == "123456"
    assert result["lat"] == 55.1
    assert result["ts"] == "2026-06-20T12:00:00Z"


def test_normalize_record_missing_source_field_becomes_none():
    result = normalize.normalize_record({"MMSI": "123456"}, MAPPING)
    assert result["object_id"] == "123456"
    assert result["lat"] is None


# ---------------------------------------------------------------------------
# validate_record -- object_id
# ---------------------------------------------------------------------------

def test_validate_record_rejects_missing_object_id():
    record = {"object_id": None, "lat": 55.1, "lon": 7.1, "ts": "2026-06-20T12:00:00Z"}
    validated, reason = normalize.validate_record(record)
    assert validated is None
    assert "object_id" in reason


def test_validate_record_rejects_empty_string_object_id():
    record = {"object_id": "  ", "lat": 55.1, "lon": 7.1, "ts": "2026-06-20T12:00:00Z"}
    validated, reason = normalize.validate_record(record)
    assert validated is None
    assert "object_id" in reason


def test_validate_record_coerces_object_id_to_string():
    record = {"object_id": 123456, "lat": 55.1, "lon": 7.1, "ts": "2026-06-20T12:00:00Z"}
    validated, reason = normalize.validate_record(record)
    assert reason is None
    assert validated["object_id"] == "123456"


# ---------------------------------------------------------------------------
# validate_record -- lat/lon
# ---------------------------------------------------------------------------

def test_validate_record_rejects_missing_lat():
    record = {"object_id": "1", "lat": None, "lon": 7.1, "ts": "2026-06-20T12:00:00Z"}
    validated, reason = normalize.validate_record(record)
    assert validated is None
    assert "lat" in reason


def test_validate_record_rejects_out_of_range_lat():
    record = {"object_id": "1", "lat": 999.0, "lon": 7.1, "ts": "2026-06-20T12:00:00Z"}
    validated, reason = normalize.validate_record(record)
    assert validated is None
    assert "out of range" in reason


def test_validate_record_rejects_non_numeric_lon():
    record = {"object_id": "1", "lat": 55.1, "lon": "not-a-number", "ts": "2026-06-20T12:00:00Z"}
    validated, reason = normalize.validate_record(record)
    assert validated is None
    assert "lon" in reason


def test_validate_record_coerces_string_coordinates_to_float():
    record = {"object_id": "1", "lat": "55.1", "lon": "7.1", "ts": "2026-06-20T12:00:00Z"}
    validated, reason = normalize.validate_record(record)
    assert reason is None
    assert validated["lat"] == 55.1
    assert isinstance(validated["lat"], float)


def test_validate_record_accepts_boundary_coordinates():
    record = {"object_id": "1", "lat": 90.0, "lon": -180.0, "ts": "2026-06-20T12:00:00Z"}
    validated, reason = normalize.validate_record(record)
    assert reason is None


# ---------------------------------------------------------------------------
# validate_record -- timestamp parsing (the core ask)
# ---------------------------------------------------------------------------

def test_validate_record_accepts_iso8601_with_z_suffix():
    record = {"object_id": "1", "lat": 55.1, "lon": 7.1, "ts": "2026-06-20T12:00:00Z"}
    validated, reason = normalize.validate_record(record)
    assert reason is None
    assert validated["ts"] == "2026-06-20T12:00:00Z"


def test_validate_record_accepts_iso8601_with_offset():
    record = {"object_id": "1", "lat": 55.1, "lon": 7.1, "ts": "2026-06-20T12:00:00+00:00"}
    validated, reason = normalize.validate_record(record)
    assert reason is None
    assert validated["ts"] == "2026-06-20T12:00:00Z"


def test_validate_record_without_ts_format_rejects_dd_mm_yyyy():
    # This is the exact failure mode confirmed earlier: TRY_CAST silently
    # returned NULL on this format with no ts_format configured. Here it
    # should fail LOUDLY (a reason string) instead of silently.
    record = {"object_id": "1", "lat": 55.1, "lon": 7.1, "ts": "20/06/2026 12:00:00"}
    validated, reason = normalize.validate_record(record)
    assert validated is None
    assert "timestamp" in reason


def test_validate_record_with_ts_format_parses_dd_mm_yyyy():
    # Same raw value as above, but now with the domain's real format
    # supplied -- this is the actual fix.
    record = {"object_id": "1", "lat": 55.1, "lon": 7.1, "ts": "20/06/2026 12:00:00"}
    validated, reason = normalize.validate_record(record, ts_format="%d/%m/%Y %H:%M:%S")
    assert reason is None
    assert validated["ts"] == "2026-06-20T12:00:00Z"  # always canonicalized to ISO 8601


def test_validate_record_accepts_epoch_seconds():
    record = {"object_id": "1", "lat": 55.1, "lon": 7.1, "ts": 1781000000}
    validated, reason = normalize.validate_record(record)
    assert reason is None
    assert validated["ts"].startswith("2026-")


def test_validate_record_accepts_epoch_milliseconds():
    record = {"object_id": "1", "lat": 55.1, "lon": 7.1, "ts": 1781000000000}
    validated, reason = normalize.validate_record(record)
    assert reason is None
    assert validated["ts"].startswith("2026-")


def test_validate_record_rejects_unparseable_timestamp():
    record = {"object_id": "1", "lat": 55.1, "lon": 7.1, "ts": "not a timestamp"}
    validated, reason = normalize.validate_record(record)
    assert validated is None
    assert "timestamp" in reason


# ---------------------------------------------------------------------------
# validate_records / normalize_and_validate -- batch behavior
# ---------------------------------------------------------------------------

def test_validate_records_partitions_valid_and_dropped():
    records = [
        {"object_id": "1", "lat": 55.1, "lon": 7.1, "ts": "2026-06-20T12:00:00Z"},   # valid
        {"object_id": "2", "lat": 999.0, "lon": 7.1, "ts": "2026-06-20T12:00:00Z"},  # bad lat
        {"object_id": None, "lat": 55.1, "lon": 7.1, "ts": "2026-06-20T12:00:00Z"},  # no object_id
    ]
    valid, dropped = normalize.validate_records(records)
    assert len(valid) == 1
    assert valid[0]["object_id"] == "1"
    assert len(dropped) == 2


def test_normalize_and_validate_full_pipeline():
    raw_records = [
        {"MMSI": "111", "Latitude": 55.1, "Longitude": 7.1,
         "# Timestamp": "20/06/2026 12:00:00", "SOG": 10.0},
        {"MMSI": "222", "Latitude": 999.0, "Longitude": 7.1,  # bad lat -- dropped
         "# Timestamp": "20/06/2026 12:00:00", "SOG": 5.0},
    ]
    valid, dropped = normalize.normalize_and_validate(
        raw_records, MAPPING, ts_format="%d/%m/%Y %H:%M:%S"
    )
    assert len(valid) == 1
    assert valid[0]["object_id"] == "111"
    assert valid[0]["ts"] == "2026-06-20T12:00:00Z"
    assert len(dropped) == 1
    assert dropped[0][0]["object_id"] == "222"  # normalized record preserved for diagnostics
    assert "lat" in dropped[0][1]  # reason mentions which field failed


if __name__ == "__main__":
    import sys, inspect
    failures = 0
    tests = [obj for name, obj in list(globals().items()) if name.startswith("test_")]
    for test in tests:
        try:
            test()
            print(f"[ok] {test.__name__}")
        except AssertionError as exc:
            failures += 1
            print(f"[FAIL] {test.__name__}: {exc}")
    print()
    if failures:
        print(f"{failures} test(s) FAILED")
        sys.exit(1)
    print(f"test_normalize.py: ALL {len(tests)} TESTS PASSED")