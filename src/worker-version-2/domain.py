"""
Loads the two configs: the canonical field mapping (domain.yml) and the optional
zones reference table (zones.yml). Both are read once at cold start by
main.py and passed down as plain arguments from there.
"""

import os
from pathlib import Path

import yaml

_BASE_DIR = Path(__file__).parent
DOMAIN_FIELD_FILE = os.getenv("DOMAIN_FIELD_FILE", str(_BASE_DIR / "configurations/domain.yml"))
ZONES_FILE = os.getenv("ZONES_FILE", str(_BASE_DIR / "configurations/zones.yml"))


def load_mapping(path: str = DOMAIN_FIELD_FILE) -> dict:
    """{canonical_field: source_field} for the active MOD domain."""
    with open(path) as f:
        return yaml.safe_load(f) or {}


def load_zones(path: str = ZONES_FILE) -> list[dict]:
    """
    Optional reference zones: [{zone_name, geom_wkt, threshold_nm}, ...].
    Returns [] if the file doesn't exist -- spatial queries that don't
    need a zones table aren't affected either way.
    """
    if not os.path.exists(path):
        return []
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    return data.get("zones", [])