from transform.utils import load_raw_files, save_processed, add_metadata


def transform_search_trends() -> str:
    """
    Transform raw Pytrends data.
    Input:  4 files, each with wide scores per keyword per date
    Output: flat list of {date, keyword, group, score} records — one row per keyword per date
    """
    print("Transforming Google Trends search data...")

    raw_files = load_raw_files("pytrends")
    if not raw_files:
        raise ValueError("No Pytrends raw files found")

    processed = []
    seen = set()  # deduplicate on (date, keyword)

    for file in raw_files:
        data  = file["data"]
        group = data.get("group", "unknown")

        for entry in data.get("data", []):
            date   = entry.get("date")
            scores = entry.get("scores", {})

            for keyword, score in scores.items():
                key = (date, keyword)
                if key in seen:
                    continue
                seen.add(key)

                clean = {
                    "date":    date,
                    "keyword": keyword,
                    "group":   group,
                    "score":   int(score) if score else 0,
                }

                processed.append(add_metadata(clean, "pytrends"))

    # Sort by date then keyword
    processed.sort(key=lambda x: (x["date"], x["keyword"]))

    filepath = save_processed(processed, "fct_search_trends")
    print(f"Transformed {len(processed)} search trend records.")
    return filepath