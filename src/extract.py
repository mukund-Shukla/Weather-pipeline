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
    return WMO_CODE_MAP.get(code,f"Unknown (code {code})")

# def fetch_weather(city_cfg,api_cfg):
#     url=api_cfg["base_url"]
#     params={
#         "latitude": city_cfg["latitude"],
#         "longitude": city_cfg["longitude"],
#         "hourly": "temperature_2m"
#     }
#     response=requests.get(url,params=params)
#     if response.status_code==200:
#         data=response.json()
#     print(data)
#     return data

# city = {
#     "name": "Lucknow",
#     "latitude": 26.8467,
#     "longitude": 80.9462
# }

# api = {
#     "base_url": "https://api.open-meteo.com/v1/forecast"
# }

# fetch_weather(city, api)


def fetch_weahter(city_cfg:dict,api_cfg:dict,watermark:str|None,logger)->dict|None:
    """
    Fetch weather data for a single city from Open-Meteo API.

    Args:
        city_cfg   : Dict with name, latitude, longitude, timezone
        api_cfg    : Dict from config['api']
        watermark  : ISO timestamp string of last successful run, or None
        logger     : Pipeline logger

    Returns:
        Raw API response as dict, or None on failure
    """

    city_name=city_cfg["name"]
    params = {
        "latitude":  city_cfg["latitude"],
        "longitude": city_cfg["longitude"],
        "timezone":  city_cfg["timezone"],
        "hourly":    ",".join(api_cfg["hourly_variables"]),
        "past_days": api_cfg["past_days"],
    }
    url = api_cfg["base_url"]
    attempts = api_cfg["retry_attempts"]
    backoff  = api_cfg["retry_backoff_seconds"]

    for attempt in range(1,attempts+1):
        try:
            logger.info(f"city={city_name} | Fetching API (attempt {attempt}/{attempts})")
            response=requests.get(url,params=params,timeout=30)
            response.raise_for_status()
            data=response.json()

            #“Log that API call was successful for this city and show how many hourly records we received.”
            logger.info(f"city={city_name} | API fetch successful | hours = {len(data.get('hourly',{}).get('time',{}))}") 
            print(data)
            return data

        except requests.exceptions.HTTPError as e:
            logger.error(f"city={city_name} | HTTP error: {e}")
        except requests.exceptions.ConnectionError:
            logger.error(f"city={city_name} | Connection error — no internet or API down")
        except requests.exceptions.Timeout:
            logger.error(f"city={city_name} | Request timed out")
        except Exception as e:
            logger.error(f"city={city_name} | Unexpected error: {e}")


        #If the current attempt failed AND we still have attempts left → wait before retrying
        if attempt<attempts:
            wait=backoff * (2**(attempt-1)) #exponential backoff: 2s,4s,8s
            logger.info(f"city={city_name} | Retrying in {wait}s...")
            time.sleep(wait)
        
        logger.error(f"city={city_name} | All {attempts} attempts failed. Skipping this city.")


# ─────────────────────────────────────────────
# SAVE RAW JSON
# ─────────────────────────────────────────────


def save_raw(raw_data:dict,city_name:str,raw_dir:str,run_id:str)->str:
    """
    Save raw API response JSON to disk.

    File naming: raw/{city}_{date}_{run_id}.json
    Returns the file path as a string.
    """
    today=datetime.now(timezone.utc).strftime("%Y-%m-%d")
    filename=f"{city_name}_{today}_{run_id}.json"
    filepath=Path(raw_dir)/filename
    
    with open(filepath,'w',encoding="utf-8") as f:
        json.dump(raw_data,f,indent=4)

    return str(filepath)
     

# ─────────────────────────────────────────────
# FLATTEN RAW → RECORDS
# ─────────────────────────────────────────────

def flatten_to_records(raw_data: dict, city_cfg: dict, source_file: str, run_id: str, watermark: str | None) -> list[dict]:
    """
    Flatten the nested Open-Meteo API response into a list of flat dicts.
    One dict = one hourly observation for one city.

    Applies watermark filter: only include records after the last run timestamp.
    """

    hourly = raw_data.get("hourly", {})
    times  = hourly.get("time", [])

    records = []
    skipped = 0

    for i, ts in enumerate(times):
        observed_at=datetime.fromisoformat(ts) #naive datetime (local city time)
        # Watermark filter = skip records already loaded in previous runs
        if watermark:
            last_run_dt=datetime.fromisoformat(watermark)
            if observed_at <=last_run_dt:
                skipped+=1
                continue

        weather_code=hourly.get("weathercode",[None]*len(times))[i]

        
        record = {
            "city_name":          city_cfg["name"],
            "latitude":           city_cfg["latitude"],
            "longitude":          city_cfg["longitude"],
            "timezone":           city_cfg["timezone"],
            "observed_at":        ts,                         # e.g. "2024-01-15T06:00"
            "temperature_c":      hourly.get("temperature_2m",       [None] * len(times))[i],
            "feels_like_c":       hourly.get("apparent_temperature", [None] * len(times))[i],
            "humidity_pct":       hourly.get("relative_humidity_2m", [None] * len(times))[i],
            "precipitation_mm":   hourly.get("precipitation",        [None] * len(times))[i],
            "windspeed_kmh":      hourly.get("windspeed_10m",        [None] * len(times))[i],
            "weather_code":       weather_code,
            "weather_description": get_weather_description(weather_code) if weather_code is not None else None,
            "pipeline_run_id":    run_id,
            "source_file_name":   Path(source_file).name,
            "ingested_at":        datetime.now(timezone.utc).isoformat(),
        }
        records.append(record)

    return records, skipped






















































































# if __name__ == "__main__":

#     city_cfg = {
#         "name": "Lucknow",
#         "latitude": 26.8467,
#         "longitude": 80.9462,
#         "timezone": "Asia/Kolkata"
#     }

#     api_cfg = {
#         "base_url": "https://api.open-meteo.com/v1/forecast",
#         "hourly_variables": ["temperature_2m"],
#         "past_days": 1,
#         "retry_attempts": 3,
#         "retry_backoff_seconds": 2
#     }

#     watermark = None

#     # simple logger
#     import logging
#     logger = logging.getLogger("test")
#     logger.setLevel(logging.INFO)
#     logger.addHandler(logging.StreamHandler())

# # Fetch
#     data = fetch_weahter(city_cfg, api_cfg, watermark, logger)
# #Save
#     file_path = save_raw(
#         raw_data=data,
#         city_name=city_cfg["name"],
#         raw_dir="data/raw",
#         run_id="test123"
#     )

#     print(f"saved file", file_path)

# #Flatten
#     # Step 3: Flatten
#     records, skipped = flatten_to_records(
#         raw_data=data,
#         city_cfg=city_cfg,
#         source_file=file_path,
#         run_id="test123",
#         watermark="2026-04-17T06:55"
#     )
#     print("Total records:", len(records))
#     print("Skipped:", skipped)
#     print("First 2 records:", records[:2])
