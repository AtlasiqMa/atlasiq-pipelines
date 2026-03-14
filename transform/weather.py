from transform.utils import load_raw_files, save_processed, add_metadata


def transform_weather() -> str:
    """
    Transform raw Open-Meteo weather data.
    Input:  one file per city, parallel arrays per metric
    Output: flat list of {city, date, temp_max, temp_min, precipitation, windspeed}
    """
    print("Transforming weather data...")

    raw_files = load_raw_files("openmeteo")
    if not raw_files:
        raise ValueError("No Open-Meteo raw files found")

    processed = []
    seen = set()  # deduplicate on (city, date)

    for file in raw_files:
        data   = file["data"]
        city   = data.get("city", "unknown")
        daily  = data.get("daily", {})

        dates         = daily.get("time", [])
        temp_max      = daily.get("temperature_2m_max", [])
        temp_min      = daily.get("temperature_2m_min", [])
        precipitation = daily.get("precipitation_sum", [])
        windspeed     = daily.get("windspeed_10m_max", [])

        # Zip parallel arrays into rows
        for i, date in enumerate(dates):
            key = (city, date)
            if key in seen:
                continue
            seen.add(key)

            clean = {
                "city":          city,
                "date":          date,
                "temp_max_c":    temp_max[i]      if i < len(temp_max)      else None,
                "temp_min_c":    temp_min[i]      if i < len(temp_min)      else None,
                "precipitation_mm": precipitation[i] if i < len(precipitation) else None,
                "windspeed_kmh": windspeed[i]     if i < len(windspeed)     else None,
            }

            processed.append(add_metadata(clean, "openmeteo"))

    # Sort by city then date
    processed.sort(key=lambda x: (x["city"], x["date"]))

    filepath = save_processed(processed, "fct_weather")
    print(f"Transformed {len(processed)} weather records.")
    return filepath