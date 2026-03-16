from transform.utils import load_raw_files, save_processed, add_metadata

from extract.config import MOROCCAN_IATAS




def get_operating_flight(record: dict) -> str:
    """
    Returns the operating flight number.
    Prefers codeshared base flight over marketing flight
    to deduplicate codeshared records.
    """
    codeshared = record.get("flight", {}).get("codeshared")
    if codeshared:
        return codeshared.get("flight_iata") or record.get("flight", {}).get("iata", "")
    return record.get("flight", {}).get("iata", "")


def classify_flight(departure_iata: str, arrival_iata: str) -> str:
    """
    Classify flight as:
    - international_arrival
    - international_departure
    - domestic
    - unknown
    """
    dep_moroccan = departure_iata in MOROCCAN_IATAS
    arr_moroccan = arrival_iata   in MOROCCAN_IATAS

    if arr_moroccan and not dep_moroccan:
        return "international_arrival"
    if dep_moroccan and not arr_moroccan:
        return "international_departure"
    if dep_moroccan and arr_moroccan:
        return "domestic"
    return "unknown"


def transform_flights() -> None:
    print("Transforming flight data...")

    raw_files = load_raw_files("aviationstack")
    if not raw_files:
        raise ValueError("No AviationStack raw files found")

    international_arrivals   = []
    international_departures = []
    domestic_flights         = []

    seen = set()  # deduplicate on operating flight + date + arrival airport

    for file in raw_files:
        records = file["data"]

        for record in records:
            departure  = record.get("departure", {}) or {}
            arrival    = record.get("arrival",   {}) or {}
            airline    = record.get("airline",   {}) or {}
            flight     = record.get("flight",    {}) or {}

            dep_iata   = departure.get("iata", "") or ""
            arr_iata   = arrival.get("iata",   "") or ""
            flight_date = record.get("flight_date", "")
            op_flight  = get_operating_flight(record)

            # Deduplicate on operating flight + date + route
            key = (op_flight, flight_date, dep_iata, arr_iata)
            if key in seen:
                continue
            seen.add(key)

            flight_type = classify_flight(dep_iata, arr_iata)
            if flight_type == "unknown":
                continue

            clean = {
                "flight_date":          flight_date,
                "flight_status":        record.get("flight_status"),
                "flight_number":        flight.get("iata"),
                "operating_flight":     op_flight,
                "airline_name":         airline.get("name"),
                "airline_iata":         airline.get("iata"),
                "departure_airport":    departure.get("airport"),
                "departure_iata":       dep_iata,
                "departure_scheduled":  departure.get("scheduled"),
                "departure_delay":      departure.get("delay"),
                "arrival_airport":      arrival.get("airport"),
                "arrival_iata":         arr_iata,
                "arrival_terminal":     arrival.get("terminal"),
                "arrival_scheduled":    arrival.get("scheduled"),
                "arrival_delay":        arrival.get("delay"),
                "flight_type":          flight_type,
            }

            clean = add_metadata(clean, "aviationstack")

            if flight_type == "international_arrival":
                international_arrivals.append(clean)
            elif flight_type == "international_departure":
                international_departures.append(clean)
            elif flight_type == "domestic":
                domestic_flights.append(clean)

    save_processed(international_arrivals,   "fct_international_arrivals")
    save_processed(international_departures, "fct_international_departures")
    save_processed(domestic_flights,         "fct_domestic_flights")

    print(f"International arrivals:   {len(international_arrivals)}")
    print(f"International departures: {len(international_departures)}")
    print(f"Domestic flights:         {len(domestic_flights)}")