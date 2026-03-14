from transform.utils import load_raw_files, save_processed, add_metadata


def transform_flights() -> str:
    """
    Transform raw AviationStack flight data.
    Input:  nested JSON with departure/arrival objects
    Output: flat list with all fields at top level
    """
    print("Transforming flight data...")

    raw_files = load_raw_files("aviationstack")
    if not raw_files:
        raise ValueError("No AviationStack raw files found")

    processed = []
    seen = set()  # deduplicate on flight_number + flight_date

    for file in raw_files:
        records = file["data"]

        for record in records:
            flight_number = record.get("flight", {}).get("iata", "")
            flight_date   = record.get("flight_date", "")

            key = (flight_number, flight_date)
            if key in seen:
                continue
            seen.add(key)

            departure = record.get("departure", {}) or {}
            arrival   = record.get("arrival", {})   or {}
            airline   = record.get("airline", {})   or {}
            flight    = record.get("flight", {})    or {}

            clean = {
                "flight_date":        flight_date,
                "flight_status":      record.get("flight_status"),
                "flight_number":      flight.get("iata"),
                "airline_name":       airline.get("name"),
                "airline_iata":       airline.get("iata"),
                "departure_airport":  departure.get("airport"),
                "departure_iata":     departure.get("iata"),
                "departure_icao":     departure.get("icao"),
                "departure_scheduled":departure.get("scheduled"),
                "departure_delay":    departure.get("delay"),
                "arrival_airport":    arrival.get("airport"),
                "arrival_iata":       arrival.get("iata"),
                "arrival_icao":       arrival.get("icao"),
                "arrival_terminal":   arrival.get("terminal"),
                "arrival_scheduled":  arrival.get("scheduled"),
                "arrival_delay":      arrival.get("delay"),
            }

            processed.append(add_metadata(clean, "aviationstack"))

    # Sort by date then flight number
    processed.sort(key=lambda x: (x["flight_date"] or "", x["flight_number"] or ""))

    filepath = save_processed(processed, "fct_flights")
    print(f"Transformed {len(processed)} flight records.")
    return filepath