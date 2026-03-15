import requests
import json
import os
import time
from datetime import datetime
from extract.utils import upload_raw_to_s3
from extract.config import MOROCCAN_AIRPORTS_EXTRACT as MOROCCAN_AIRPORTS


AVIATIONSTACK_BASE_URL = "http://api.aviationstack.com/v1"
RAW_DATA_PATH = "/opt/airflow/data/raw/aviationstack"




def fetch_arrivals(airport_iata: str, limit: int = 100) -> list:
    api_key = os.getenv("AVIATIONSTACK_API_KEY")
    params  = {
        "access_key": api_key,
        "arr_iata":   airport_iata,
        "limit":      limit,
    }
    response = requests.get(f"{AVIATIONSTACK_BASE_URL}/flights", params=params)
    response.raise_for_status()
    records = response.json().get("data", [])

    # Tag each record with direction and query airport
    for r in records:
        r["_direction"]      = "arrival"
        r["_query_airport"]  = airport_iata

    return records


def fetch_departures(airport_iata: str, limit: int = 100) -> list:
    api_key = os.getenv("AVIATIONSTACK_API_KEY")
    params  = {
        "access_key": api_key,
        "dep_iata":   airport_iata,
        "limit":      limit,
    }
    response = requests.get(f"{AVIATIONSTACK_BASE_URL}/flights", params=params)
    response.raise_for_status()
    records = response.json().get("data", [])

    # Tag each record with direction and query airport
    for r in records:
        r["_direction"]      = "departure"
        r["_query_airport"]  = airport_iata

    return records


def save_raw(data: list, airport_name: str, direction: str) -> str:
    os.makedirs(RAW_DATA_PATH, exist_ok=True)
    today    = datetime.now().strftime("%Y-%m-%d")
    filename = f"{direction}_{airport_name.lower()}_{today}.json"
    filepath = os.path.join(RAW_DATA_PATH, filename)
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Saved {len(data)} {direction} for {airport_name} to {filepath}")
    upload_raw_to_s3(filepath, "aviationstack")
    return filepath


def extract_aviationstack_flights() -> None:
    print("Extracting flight data from AviationStack...")

    for city, iata in MOROCCAN_AIRPORTS.items():
        try:
            print(f"Fetching arrivals for {city} ({iata})...")
            arrivals = fetch_arrivals(iata)
            save_raw(arrivals, city, "arrivals")
            time.sleep(2)
        except Exception as e:
            print(f"Failed arrivals for {city}: {e}")

        try:
            print(f"Fetching departures for {city} ({iata})...")
            departures = fetch_departures(iata)
            save_raw(departures, city, "departures")
            time.sleep(2)
        except Exception as e:
            print(f"Failed departures for {city}: {e}")

    print("AviationStack extraction complete.")