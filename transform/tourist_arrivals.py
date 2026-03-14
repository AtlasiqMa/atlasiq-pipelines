from transform.utils import load_raw_files, save_processed, add_metadata


def transform_tourist_arrivals() -> str:
    """
    Transform raw World Bank tourist arrivals data.
    Input:  list of yearly records with nested indicator/country objects
    Output: flat list of {year, arrivals} records
    """
    print("Transforming tourist arrivals data...")

    raw_files = load_raw_files("worldbank")
    if not raw_files:
        raise ValueError("No World Bank raw files found")

    processed = []
    seen_years = set()

    for file in raw_files:
        records = file["data"]

        for record in records:
            year     = record.get("date")
            arrivals = record.get("value")

            if not year or arrivals is None:
                continue
            if year in seen_years:
                continue

            seen_years.add(year)

            clean = {
                "year":         int(year),
                "arrivals":     int(arrivals),
                "country_code": record.get("countryiso3code", "MAR"),
                "country_name": "Morocco",
                "indicator_id": record.get("indicator", {}).get("id", "ST.INT.ARVL"),
            }

            processed.append(add_metadata(clean, "worldbank"))

    processed.sort(key=lambda x: x["year"])

    filepath = save_processed(processed, "fct_tourist_arrivals")
    print(f"Transformed {len(processed)} yearly arrival records.")
    return filepath