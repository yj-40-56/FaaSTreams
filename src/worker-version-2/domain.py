"""
Loads the two configs: the canonical field mapping (domain.yml) and the optional
zones reference table (zones.yml). Both are read once at cold start by
main.py and passed down as plain arguments from there.
"""

import os
from pathlib import Path

import yaml

from normalize import CANONICAL_FIELDS

_BASE_DIR = Path(__file__).parent
DOMAIN_FIELD_FILE = os.getenv("DOMAIN_FIELD_FILE", str(_BASE_DIR / "domain.yml"))
ZONES_FILE = os.getenv("ZONES_FILE", str(_BASE_DIR / "zones.yml"))


def load_mapping(path: str = DOMAIN_FIELD_FILE) -> dict:
    """{canonical_field: source_field} for the active MOD domain. Filtered
    to known canonical fields only, so a sibling key like `ts_format`
    (see load_ts_format) isn't mistaken for a field to rename."""
    with open(path) as f:
        raw = yaml.safe_load(f) or {}
    return {k: v for k, v in raw.items() if k in CANONICAL_FIELDS}


def load_ts_format(path: str = DOMAIN_FIELD_FILE) -> str | None:
    """
    Optional strptime format for this domain's raw timestamp string, e.g.
    "%d/%m/%Y %H:%M:%S". Returns None if domain.yml doesn't set one --
    normalize.py then assumes the raw timestamp is already ISO 8601.
    """
    with open(path) as f:
        raw = yaml.safe_load(f) or {}
    return raw.get("ts_format")


def load_zones(path: str = ZONES_FILE) -> list[dict]:
    """
    Optional reference zones: [{zone_name, geom_wkt, threshold_nm}, ...].
    Returns [] if the file doesn't exist, is empty, or fails to parse --
    zones are optional, so a problem here must never be able to crash
    worker startup (this function runs at import time in main.py).
    Accepts either a flat top-level list, or a dict with a "zones" key
    wrapping the list -- both are common conventions, and which one a
    given config file uses shouldn't matter.
    """
    if not os.path.exists(path):
        return []

    try:
        with open(path) as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as exc:
        print(f"Warning: could not parse zones file {path}: {exc}", flush=True)
        return []

    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("zones", [])
    return []