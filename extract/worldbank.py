import requests
import json
import os
from datetime import datetime


WORLDBANK_BASE_URL = "https://api.worldbank.org/v2"
COUNTRY_CODE = "MA"
RAW_DATA_PATH = "/opt/airflow/data/raw/worldbank"


def fetch_indicator(indicator: str, per_page: int = 100) -> list:
    """
    Fetch a single indicator for Morocco from the World Bank API.
    Returns a list of yearly records.
    """
    url = f"{WORLDBANK_BASE_URL}/country/{COUNTRY_CODE}/indicator/{indicator}"
    params = {
        "format": "json",
        "per_page": per_page
    }

    response = requests.get(url, params=params)
    response.raise_for_status()

    data = response.json()

    # World Bank returns [metadata, records]
    # index 0 = pagination metadata
    # index 1 = actual records
    records = data[1]

    # Clean out years with no data
    records = [r for r in records if r["value"] is not None]

    return records


def save_raw(data: list, indicator: str) -> str:
    """
    Save raw JSON response to local storage.
    Returns the path where the file was saved.
    """
    os.makedirs(RAW_DATA_PATH, exist_ok=True)

    today = datetime.now().strftime("%Y-%m-%d")
    filename = f"{indicator}_{today}.json"
    filepath = os.path.join(RAW_DATA_PATH, filename)

    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Saved {len(data)} records to {filepath}")
    return filepath


def extract_tourist_arrivals() -> str:
    """
    Main extraction function for tourist arrivals.
    Called by Airflow DAG.
    """
    print("Extracting Morocco tourist arrivals from World Bank...")
    records = fetch_indicator("ST.INT.ARVL")
    filepath = save_raw(records, "ST.INT.ARVL")
    print(f"Extraction complete. {len(records)} years of data saved.")
    return filepath