"""Backward-compat shim — canonical ROOT now lives in top-level _root.py."""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
