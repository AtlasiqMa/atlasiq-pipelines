import requests
import json
import os
from datetime import datetime, timedelta


OPENSKY_TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
OPENSKY_BASE_URL = "https://opensky-network.org/api"
RAW_DATA_PATH = "/opt/airflow/data/raw/opensky"

# Moroccan airports ICAO codes
MOROCCAN_AIRPORTS = {
    "Casablanca":  "GMMN",
    "Marrakech":   "GMMX",
    "Agadir":      "GMAD",
    "Tangier":     "GMTT",
    "Fes":         "GMFF",
    "Rabat":       "GMME",
}


def get_token() -> str:
    """
    Authenticate with OpenSky using OAuth2 client credentials flow.
    Returns a bearer token valid for 30 minutes.
    """
    client_id     = os.getenv("OPENSKY_CLIENT_ID")
    client_secret = os.getenv("OPENSKY_CLIENT_SECRET")

    response = requests.post(
        OPENSKY_TOKEN_URL,
        data={
            "grant_type":    "client_credentials",
            "client_id":     client_id,
            "client_secret": client_secret,
        }
    )
    response.raise_for_status()
    token = response.json()["access_token"]
    print("OpenSky token acquired.")
    return token


def fetch_arrivals(airport_icao: str, token: str, days_back: int = 1) -> list:
    """
    Fetch arriving flights for a given Moroccan airport.
    Defaults to the last 24 hours.
    """
    end   = datetime.utcnow()
    start = end - timedelta(days=days_back)

    params = {
        "airport": airport_icao,
        "begin":   int(start.timestamp()),
        "end":     int(end.timestamp()),
    }

    headers = {"Authorization": f"Bearer {token}"}

    response = requests.get(
        f"{OPENSKY_BASE_URL}/flights/arrival",
        params=params,
        headers=headers
    )
    response.raise_for_status()
    return response.json()


def save_raw(data: list, airport_name: str) -> str:
    """
    Save raw flight JSON to the data lake.
    """
    os.makedirs(RAW_DATA_PATH, exist_ok=True)

    today    = datetime.now().strftime("%Y-%m-%d")
    filename = f"arrivals_{airport_name.lower()}_{today}.json"
    filepath = os.path.join(RAW_DATA_PATH, filename)

    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Saved {len(data)} flights for {airport_name} to {filepath}")
    return filepath


def extract_flight_arrivals() -> None:
    """
    Main extraction function called by Airflow DAG.
    Loops through all Moroccan airports and saves raw arrivals.
    """
    print("Extracting flight arrivals for Moroccan airports...")
    token = get_token()

    for city, icao in MOROCCAN_AIRPORTS.items():
        try:
            print(f"Fetching arrivals for {city} ({icao})...")
            flights = fetch_arrivals(icao, token)
            save_raw(flights, city)
        except Exception as e:
            print(f"Failed to fetch {city}: {e}")
            continue

    print("Flight extraction complete.")