import pytest
from datetime import datetime
from pathlib import Path
from worker.connection import create_connection
from worker.ingestion import ingest_csv
from worker.queries import (
    vessels_in_bbox,
    vessel_activity_in_window,
    vessels_in_zone_during_window,
)
from worker.models import BoundingBox, TimeWindow, ZoneIntrusionParams
from config import AIS_CSV

CSV_PATH = AIS_CSV


@pytest.fixture
def conn():
    c = create_connection()
    ingest_csv(c, CSV_PATH)
    return c


# ─── Spatial ──────────────────────────────────────────────────────────────────

def test_spatial_returns_vessels_inside_box(conn):
    bbox = BoundingBox(lat_min=54.0, lat_max=56.0, lon_min=8.0, lon_max=13.0)
    results = vessels_in_bbox(conn, bbox)

    print(f"\n[spatial] {len(results)} vessels found in bbox")
    for r in results:
        print(f"  MMSI={r.mmsi} name={r.name} lat={r.lat} lon={r.lon} sog={r.sog}")

    assert len(results) > 0
    for r in results:
        assert bbox.lat_min <= r.lat <= bbox.lat_max
        assert bbox.lon_min <= r.lon <= bbox.lon_max


def test_spatial_returns_empty_for_box_with_no_vessels(conn):
    bbox = BoundingBox(lat_min=0.0, lat_max=1.0, lon_min=0.0, lon_max=1.0)
    results = vessels_in_bbox(conn, bbox)

    print(f"\n[spatial] {len(results)} vessels found in empty bbox (expected 0)")

    assert results == []


# ─── Temporal ─────────────────────────────────────────────────────────────────

def test_temporal_returns_summaries_in_window(conn):
    window = TimeWindow(
        start=datetime(2026, 4, 26, 0, 0, 0),
        end=datetime(2026, 4, 26, 23, 59, 59),
    )
    results = vessel_activity_in_window(conn, window)

    print(f"\n[temporal] {len(results)} vessels active in window")
    for r in results:
        avg_sog = f"{r.avg_speed_knots:.1f}" if r.avg_speed_knots is not None else "N/A"
        print(f"  MMSI={r.mmsi} name={r.name} pings={r.ping_count} avg_sog={avg_sog} first={r.first_seen} last={r.last_seen}")
    assert len(results) > 0
    for r in results:
        assert r.ping_count >= 1
        assert r.first_seen <= r.last_seen


def test_temporal_returns_empty_outside_data_range(conn):
    window = TimeWindow(
        start=datetime(2020, 1, 1),
        end=datetime(2020, 1, 2),
    )
    results = vessel_activity_in_window(conn, window)

    print(f"\n[temporal] {len(results)} vessels in out-of-range window (expected 0)")

    assert results == []


# ─── Spatio-temporal ──────────────────────────────────────────────────────────

def test_spatio_temporal_combines_filters(conn):
    params = ZoneIntrusionParams(
        bbox=BoundingBox(lat_min=55.0, lat_max=56.0, lon_min=10.0, lon_max=11.0),
        window=TimeWindow(
            start=datetime(2026, 4, 26, 0, 0, 0),
            end=datetime(2026, 4, 26, 23, 59, 59),
        ),
    )
    results = vessels_in_zone_during_window(conn, params)

    print(f"\n[spatio-temporal] {len(results)} pings in zone+window")
    for r in results:
        print(f"  MMSI={r.mmsi} name={r.name} lat={r.lat} lon={r.lon} ts={r.timestamp}")

    for r in results:
        assert params.bbox.lat_min <= r.lat <= params.bbox.lat_max
        assert params.window.start <= r.timestamp <= params.window.end


def test_spatio_temporal_nav_status_filter(conn):
    params = ZoneIntrusionParams(
        bbox=BoundingBox(lat_min=54.0, lat_max=57.0, lon_min=8.0, lon_max=13.0),
        window=TimeWindow(
            start=datetime(2026, 4, 26, 0, 0, 0),
            end=datetime(2026, 4, 26, 23, 59, 59),
        ),
        nav_status_filter="Engaged in fishing",
    )
    results = vessels_in_zone_during_window(conn, params)

    print(f"\n[spatio-temporal] {len(results)} fishing vessels in zone+window")
    for r in results:
        print(f"  MMSI={r.mmsi} name={r.name} status={r.nav_status} ts={r.timestamp}")

    assert all(r.nav_status == "Engaged in fishing" for r in results)