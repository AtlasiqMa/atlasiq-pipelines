import requests
import json
import os
from datetime import datetime
from extract.utils import upload_raw_to_s3
import time




AVIATIONSTACK_BASE_URL = "http://api.aviationstack.com/v1"
RAW_DATA_PATH = "/opt/airflow/data/raw/aviationstack"

# Moroccan airports IATA codes
MOROCCAN_AIRPORTS = {
    "Casablanca":  "CMN",
    "Marrakech":   "RAK",
    "Agadir":      "AGA",
    "Tangier":     "TNG",
    "Fes":         "FEZ",
    "Rabat":       "RBA",
    "Oujda":       "OUD",
    "Essaouira":   "ESU",
}


def fetch_arrivals(airport_iata: str, limit: int = 100) -> list:
    """
    Fetch arriving flights for a given Moroccan airport.
    """
    api_key = os.getenv("AVIATIONSTACK_API_KEY")

    params = {
        "access_key": api_key,
        "arr_iata":   airport_iata,
        "limit":      limit,
    }

    response = requests.get(
        f"{AVIATIONSTACK_BASE_URL}/flights",
        params=params
    )
    response.raise_for_status()

    data = response.json()

    # AviationStack returns {"pagination": {...}, "data": [...]}
    records = data.get("data", [])
    return records


def fetch_departures(airport_iata: str, limit: int = 100) -> list:
    """
    Fetch departing flights from a given Moroccan airport.
    """
    api_key = os.getenv("AVIATIONSTACK_API_KEY")

    params = {
        "access_key": api_key,
        "dep_iata":   airport_iata,
        "limit":      limit,
    }

    response = requests.get(
        f"{AVIATIONSTACK_BASE_URL}/flights",
        params=params
    )
    response.raise_for_status()

    data = response.json()
    records = data.get("data", [])
    return records


def save_raw(data: list, airport_name: str, direction: str) -> str:
    """
    Save raw flight JSON to the data lake.
    direction: 'arrivals' or 'departures'
    """
    os.makedirs(RAW_DATA_PATH, exist_ok=True)

    today    = datetime.now().strftime("%Y-%m-%d")
    filename = f"{direction}_{airport_name.lower()}_{today}.json"
    filepath = os.path.join(RAW_DATA_PATH, filename)

    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Saved {len(data)} {direction} for {airport_name} to {filepath}")
    return filepath


def extract_aviationstack_flights() -> None:
    """
    Main extraction function called by Airflow DAG.
    Fetches arrivals and departures for all Moroccan airports.
    """
    print("Extracting flight data from AviationStack...")

    
    for city, iata in MOROCCAN_AIRPORTS.items():
        try:
            arrivals = fetch_arrivals(iata)
            save_raw(arrivals, city, "arrivals")
            time.sleep(2)  # 2 seconds between requests
        except Exception as e:
            print(f"Failed arrivals for {city}: {e}")

        try:
            departures = fetch_departures(iata)
            save_raw(departures, city, "departures")
            time.sleep(2)
        except Exception as e:
            print(f"Failed departures for {city}: {e}")

    print("AviationStack extraction complete.")


def save_raw(data: list, airport_name: str, direction: str) -> str:
    os.makedirs(RAW_DATA_PATH, exist_ok=True)
    today    = datetime.now().strftime("%Y-%m-%d")
    filename = f"{direction}_{airport_name.lower()}_{today}.json"
    filepath = os.path.join(RAW_DATA_PATH, filename)
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)
    upload_raw_to_s3(filepath, "aviationstack")
    return filepath