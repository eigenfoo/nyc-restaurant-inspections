import pandas as pd


if __name__ == "__main__":
    # Load inspection data: both historical and for today.
    print("Loading historical data...")
    historical_inspections = pd.read_parquet(
        "data/inspections.parquet",
    )
    print("Fetching today's data...")
    new_inspections = pd.read_csv(
        "https://data.cityofnewyork.us/api/views/43nn-pn8j/rows.csv?accessType=DOWNLOAD"
    )

    today = pd.to_datetime("today").normalize().strftime("%Y-%m-%d")
    new_inspections["first_seen_datetime"] = today
    new_inspections["last_seen_datetime"] = today

    print("Updating historical data...")
    # Concatenate both datasets, keeping only new rows.
    subset = list(historical_inspections.columns)
    subset.remove("first_seen_datetime")
    subset.remove("last_seen_datetime")
    combined_inspections = pd.concat(
        [historical_inspections, new_inspections]
    ).drop_duplicates(subset=subset, keep="first")

    print("Writing...")
    combined_inspections.to_parquet("data/inspections.parquet", index=False)
