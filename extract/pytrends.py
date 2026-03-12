import json
import os
import time
from datetime import datetime

import pandas as pd
from pytrends.request import TrendReq


RAW_DATA_PATH = "/opt/airflow/data/raw/pytrends"

# Keywords grouped by theme
# We query in small groups because Google Trends compares
# keywords relative to each other — max 5 per request
KEYWORD_GROUPS = {
    "morocco_general": [
        "Morocco travel",
        "Visit Morocco",
        "Morocco tourism",
        "Morocco vacation",
        "Morocco holiday",
    ],
    "cities": [
        "Marrakech",
        "Agadir beach",
        "Fes Morocco",
        "Tangier Morocco",
        "Essaouira Morocco",
    ],
    "accommodations": [
        "Marrakech hotel",
        "Agadir hotel",
        "Morocco riad",
        "Morocco Airbnb",
        "Morocco resort",
    ],
    "experiences": [
        "Sahara desert tour Morocco",
        "Morocco food",
        "Morocco surf",
        "Morocco hiking",
        "Morocco digital nomad",
    ],
}


def fetch_trends(group_name: str, keywords: list) -> dict:
    """
    Fetch Google Trends interest over time for a keyword group.
    Returns a dict with dates and scores per keyword.
    """
    pytrends = TrendReq(hl="en-US", tz=0)

    pytrends.build_payload(
        kw_list=keywords,
        timeframe="today 12-m",
        geo="",
    )

    df = pytrends.interest_over_time()

    if df.empty:
        print(f"No data returned for group: {group_name}")
        return {}

    # Drop isPartial column
    if "isPartial" in df.columns:
        df = df.drop(columns=["isPartial"])

    # Convert to JSON-serializable format
    result = {
        "group":        group_name,
        "keywords":     keywords,
        "timeframe":    "today 12-m",
        "extracted_at": datetime.now().isoformat(),
        "data": []
    }

    for date, row in df.iterrows():
        result["data"].append({
            "date":   date.strftime("%Y-%m-%d"),
            "scores": row.to_dict()
        })

    return result


def save_raw(data: dict, group_name: str) -> str:
    """
    Save raw trends JSON to the data lake.
    """
    os.makedirs(RAW_DATA_PATH, exist_ok=True)

    today    = datetime.now().strftime("%Y-%m-%d")
    filename = f"trends_{group_name}_{today}.json"
    filepath = os.path.join(RAW_DATA_PATH, filename)

    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Saved trends for {group_name} to {filepath}")
    return filepath


def extract_google_trends() -> None:
    """
    Main extraction function called by Airflow DAG.
    Fetches Google Trends for all keyword groups.
    """
    print("Extracting Google Trends for Morocco tourism keywords...")

    for group_name, keywords in KEYWORD_GROUPS.items():
        try:
            print(f"Fetching trends for group: {group_name}...")
            data = fetch_trends(group_name, keywords)

            if data:
                save_raw(data, group_name)

            # Sleep between requests to avoid Google rate limiting
            print("Waiting 10 seconds to avoid rate limiting...")
            time.sleep(10)

        except Exception as e:
            print(f"Failed trends for {group_name}: {e}")
            continue

    print("Google Trends extraction complete.")