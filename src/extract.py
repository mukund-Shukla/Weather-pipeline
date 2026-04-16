"""
extract.py
----------
Responsible for:
  - Calling the Open-Meteo API for each city
  - Retry logic with exponential backoff
  - Saving raw JSON response to local disk
  - Returning structured data for downstream processing
"""


import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests


# ─────────────────────────────────────────────
# WMO WEATHER CODE MAPPING
# ─────────────────────────────────────────────

WMO_CODE_MAP = {
    0:  "Clear sky",
    1:  "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Icy fog",
    51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
    61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
    71: "Slight snow", 73: "Moderate snow", 75: "Heavy snow",
    77: "Snow grains",
    80: "Slight showers", 81: "Moderate showers", 82: "Violent showers",
    85: "Slight snow showers", 86: "Heavy snow showers",
    95: "Thunderstorm", 96: "Thunderstorm with hail", 99: "Thunderstorm with heavy hail",
}

def get_weather_description(code:int)->str:
    """Map a WMO weather code to a human-readable description"""
    