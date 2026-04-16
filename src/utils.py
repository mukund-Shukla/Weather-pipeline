"""
utils.py
--------
Shared utilities used across all pipeline modules.
Covers: config loading, logger setup, watermark read/write.
"""

import json
import logging
import os
import uuid
from datetime import datetime
from pathlib import Path

import yaml
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

def load_config(config_path: str = "config/config.yaml") -> dict:
    """Load and return the YAML config as a dict."""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


# ─────────────────────────────────────────────
# LOGGER
# ─────────────────────────────────────────────

def setup_logger(log_file: str, run_id: str) -> logging.Logger:
    """
    Set up a logger that writes to both console and a log file.
    Each log line includes the pipeline_run_id for traceability.
    """
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("weather_pipeline")
    logger.setLevel(logging.DEBUG)

    # Avoid duplicate handlers if logger already exists
    if logger.handlers:
        logger.handlers.clear()

    formatter = logging.Formatter(
        f"%(asctime)s | %(levelname)-5s | run_id={run_id} | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # File handler
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


# ─────────────────────────────────────────────
# RUN ID
# ─────────────────────────────────────────────

def generate_run_id() -> str:
    """Generate a unique pipeline run ID (short UUID)."""
    return str(uuid.uuid4())[:8]


# ─────────────────────────────────────────────
# WATERMARK
# ─────────────────────────────────────────────

def load_watermark(watermark_file: str, cities: list) -> dict:
    """
    Load the watermark file. If it doesn't exist or a city is missing,
    defaults to None (meaning full historical load for that city).
    """
    watermark_path = Path(watermark_file)

    if not watermark_path.exists():
        return {city: None for city in cities}

    with open(watermark_path, "r") as f:
        stored = json.load(f)

    # Ensure all cities are present (handles new city added to config)
    watermark = {}
    for city in cities:
        watermark[city] = stored.get(city, None)

    return watermark


def save_watermark(watermark_file: str, watermark: dict) -> None:
    """
    Persist the updated watermark to disk.
    Only called after a confirmed successful Snowflake load.
    """
    watermark_path = Path(watermark_file)
    watermark_path.parent.mkdir(parents=True, exist_ok=True)

    with open(watermark_path, "w") as f:
        json.dump(watermark, f, indent=2)


# ─────────────────────────────────────────────
# DIRECTORY SETUP
# ─────────────────────────────────────────────

def ensure_directories(config: dict) -> None:
    """Create all required local directories if they don't exist."""
    pipeline_cfg = config["pipeline"]
    dirs = [
        pipeline_cfg["local_raw_dir"],
        pipeline_cfg["local_staging_dir"],
        pipeline_cfg["local_processed_dir"],
        "watermark",
        Path(pipeline_cfg["log_file"]).parent,
    ]
    for d in dirs:
        Path(d).mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────
# SNOWFLAKE CREDENTIALS
# ─────────────────────────────────────────────

def get_snowflake_creds() -> dict:
    """Pull Snowflake credentials from environment variables."""
    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_ROLE"]
    creds = {}

    for key in required:
        val = os.getenv(key)
        if not val:
            raise EnvironmentError(f"Missing required env variable: {key}")
        creds[key.lower()] = val

    return creds
