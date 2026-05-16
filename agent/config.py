"""
Runtime configuration — environment-driven settings.

Railway setup:
  1. Dashboard → your service → Volumes → Add Volume → mount at /data
  2. Variables → DATA_DIR=/data
"""
import os
from pathlib import Path

DATA_DIR = Path(os.environ.get("DATA_DIR", "data"))
PAPER_BALANCE_INITIAL = float(os.environ.get("PAPER_BALANCE", "1000"))
KALSHI_AGENT_URL = os.environ.get("KALSHI_AGENT_URL", "")
