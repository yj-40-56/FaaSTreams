"""
Defines the shapes of data going in and out of queries.
"""

from datetime import datetime
from pydantic import BaseModel, field_validator


# query param classes

class BoundingBox(BaseModel):
    lat_min: float
    lat_max: float
    lon_min: float
    lon_max: float

    @field_validator("lat_min", "lat_max")
    @classmethod
    def valid_latitude(cls, v: float) -> float:
        if not -90 <= v <= 90:
            raise ValueError(f"Latitude {v} out of range [-90, 90]")
        return v

    @field_validator("lon_min", "lon_max")
    @classmethod
    def valid_longitude(cls, v: float) -> float:
        if not -180 <= v <= 180:
            raise ValueError(f"Longitude {v} out of range [-180, 180]")
        return v


class TimeWindow(BaseModel):
    start: datetime
    end: datetime

    @field_validator("end")
    @classmethod
    def end_after_start(cls, v: datetime, info) -> datetime:
        if "start" in info.data and v <= info.data["start"]:
            raise ValueError("end must be after start")
        return v


class ZoneIntrusionParams(BaseModel):
    bbox: BoundingBox
    window: TimeWindow
    nav_status_filter: str | None = None  # e.g. "Engaged in fishing"


# result classes

class VesselPing(BaseModel):
    mmsi: int
    name: str | None
    ship_type: str | None
    nav_status: str | None
    lat: float
    lon: float
    sog: float | None
    timestamp: datetime


class VesselSummary(BaseModel):
    mmsi: int
    name: str | None
    ping_count: int
    first_seen: datetime
    last_seen: datetime
    avg_speed_knots: float | None
    max_speed_knots: float | None