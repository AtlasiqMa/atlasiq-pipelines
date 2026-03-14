import requests
import json
import os
from datetime import datetime
from extract.utils import upload_raw_to_s3



OPENMETEO_BASE_URL = "https://api.open-meteo.com/v1/forecast"
RAW_DATA_PATH = "/opt/airflow/data/raw/openmeteo"

# Moroccan cities with coordinates
MOROCCAN_CITIES = {
    "Casablanca":  {"latitude": 33.5731, "longitude": -7.5898},
    "Marrakech":   {"latitude": 31.6295, "longitude": -7.9811},
    "Agadir":      {"latitude": 30.4278, "longitude": -9.5981},
    "Tangier":     {"latitude": 35.7595, "longitude": -5.8340},
    "Fes":         {"latitude": 34.0181, "longitude": -5.0078},
    "Rabat":       {"latitude": 34.0209, "longitude": -6.8416},
    "Essaouira":   {"latitude": 31.5085, "longitude": -9.7595},
    "Oujda":       {"latitude": 34.6867, "longitude": -1.9114},
}


def fetch_weather(city: str, coords: dict) -> dict:
    """
    Fetch 7-day weather forecast for a given Moroccan city.
    """
    params = {
        "latitude":   coords["latitude"],
        "longitude":  coords["longitude"],
        "daily":      "temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max",
        "timezone":   "Africa/Casablanca",
    }

    response = requests.get(OPENMETEO_BASE_URL, params=params)
    response.raise_for_status()

    data = response.json()

    # Enrich with city name for clarity downstream
    data["city"] = city
    data["extracted_at"] = datetime.now().isoformat()

    return data


def save_raw(data: dict, city: str) -> str:
    """
    Save raw weather JSON to the data lake.
    """
    os.makedirs(RAW_DATA_PATH, exist_ok=True)

    today    = datetime.now().strftime("%Y-%m-%d")
    filename = f"weather_{city.lower()}_{today}.json"
    filepath = os.path.join(RAW_DATA_PATH, filename)

    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Saved weather for {city} to {filepath}")
    return filepath


def extract_weather() -> None:
    """
    Main extraction function called by Airflow DAG.
    Fetches weather for all Moroccan cities.
    """
    print("Extracting weather data for Moroccan cities...")

    for city, coords in MOROCCAN_CITIES.items():
        try:
            print(f"Fetching weather for {city}...")
            data = fetch_weather(city, coords)
            save_raw(data, city)
        except Exception as e:
            print(f"Failed weather for {city}: {e}")
            continue

    print("Weather extraction complete.")


def save_raw(data: dict, city: str) -> str:
    os.makedirs(RAW_DATA_PATH, exist_ok=True)
    today    = datetime.now().strftime("%Y-%m-%d")
    filename = f"weather_{city.lower()}_{today}.json"
    filepath = os.path.join(RAW_DATA_PATH, filename)
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)
    upload_raw_to_s3(filepath, "openmeteo")
    return filepath