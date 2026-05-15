"""
Ingestion job: Pull new Seattle Fire 911 calls from the Seattle Open Data API
and append to the landing Volume as Delta.

Runs as a Lakeflow Job task (scheduled or manual).
Uses a high-water mark stored in the Volume to only fetch new records.

API docs (SODA v2): https://dev.socrata.com/foundry/data.seattle.gov/xvj2-ai6y
Dataset updates every 5 minutes.
Fields: address, type, datetime, latitude, longitude, report_location, incident_number
"""

import os
import requests
import pandas as pd
from datetime import datetime, timezone
from pyspark.sql import SparkSession


API_URL = "https://data.seattle.gov/resource/xvj2-ai6y.json"
VOLUME_PATH = "/Volumes/seattle_data/seattle_landing/seattle_fire/UST_records/"

WATERMARK_PATH = "/Volumes/seattle_data/seattle_landing/seattle_fire/UST_records_watermark/"
PAGE_SIZE = 50000
DATETIME_FIELD = "datetime"
APP_TOKEN = os.environ.get("SOCRATA_APP_TOKEN", "")


def get_high_water_mark(spark: SparkSession) -> str | None:
    """Read the last ingested datetime from the watermark Delta table."""
    try:
        df = spark.read.format("delta").load(WATERMARK_PATH)
        row = df.orderBy(df.watermark.desc()).first()
        return row.watermark if row else None
    except Exception:
        return None


def save_high_water_mark(spark: SparkSession, watermark: str) -> None:
    """Persist the new high-water mark."""
    df = spark.createDataFrame(
        [(watermark, datetime.now(timezone.utc).isoformat())],
        ["watermark", "updated_at"],
    )
    df.write.format("delta").mode("overwrite").save(WATERMARK_PATH)


def fetch_new_records(since: str | None) -> pd.DataFrame:
    """Fetch records from the SODA v2 API newer than `since`, with pagination."""
    all_records: list[dict] = []
    offset = 0

    headers = {}
    if APP_TOKEN:
        headers["X-App-Token"] = APP_TOKEN

    while True:
        params = {
            "$select": "address, type, datetime, latitude, longitude, incident_number",
            "$order": "datetime ASC",
            "$limit": PAGE_SIZE,
            "$offset": offset,
        }
        if since:
            params["$where"] = f"datetime > '{since}'"

        response = requests.get(API_URL, params=params, headers=headers, timeout=120)
        response.raise_for_status()
        batch = response.json()

        if not batch:
            break

        all_records.extend(batch)

        if len(batch) < PAGE_SIZE:
            break

        offset += PAGE_SIZE

    return pd.DataFrame(all_records)


def main():
    spark = SparkSession.builder.getOrCreate()

    # 1. Read high-water mark (last ingested datetime)
    hwm = get_high_water_mark(spark)
    print(f"High-water mark: {hwm or 'None (first run, full load)'}")

    # 2. Fetch only new records from the API
    pdf = fetch_new_records(since=hwm)

    if pdf.empty:
        print("No new records found. Exiting.")
        return

    # 3. Append to the landing Volume
    spark_df = spark.createDataFrame(pdf)
    spark_df.write.format("delta").mode("append").save(VOLUME_PATH)

    # 4. Update the high-water mark
    new_hwm = pdf[DATETIME_FIELD].max()
    save_high_water_mark(spark, new_hwm)

    print(f"Appended {len(pdf)} rows. New high-water mark: {new_hwm}")


if __name__ == "__main__":
    main()
