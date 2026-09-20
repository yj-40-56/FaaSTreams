#!/usr/bin/env python3
"""Fail before cloud operations if PROJECT and the initialized backend disagree."""
import json
import os
from pathlib import Path
import re
import sys


def fail(message):
    sys.exit(f"Terraform context error: {message}")


project = os.environ.get("PROJECT", "")
if not re.fullmatch(r"[a-z][a-z0-9-]{4,28}[a-z0-9]", project):
    fail("set PROJECT to an explicit GCP project ID (for example, export PROJECT=faas-pj).")

if "--project-only" in sys.argv:
    sys.exit(0)

data_dir = Path(os.environ.get("TF_DATA_DIR") or ".terraform")
try:
    backend = json.loads((data_dir / "terraform.tfstate").read_text())["backend"]
except (OSError, ValueError, KeyError, TypeError):
    fail(f"backend metadata is missing or unreadable; run make init PROJECT={project} in terraform/.")

expected = f"{project}-terraform-state"
if (backend.get("type") != "gcs"
        or backend.get("config", {}).get("bucket") != expected
        or backend.get("config", {}).get("prefix") != "terraform/state"):
    fail(f"backend must be gs://{expected}/terraform/state; run make init PROJECT={project} in terraform/.")

# This stack uses one backend per project, not Terraform workspaces.
workspace = os.environ.get("TF_WORKSPACE")
if not workspace:
    try:
        workspace = (data_dir / "environment").read_text().strip()
    except FileNotFoundError:
        workspace = "default"
if workspace != "default":
    fail("use the default Terraform workspace; projects are selected with PROJECT and make init.")
