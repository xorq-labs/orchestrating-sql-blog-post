# dags/weather_pipeline.py
from airflow.decorators import dag, task
import pendulum
import os
import sqlite3
import logging
import requests
from urllib.parse import quote_plus
import json
import pandas as pd
import duckdb

DB_PATH   = os.getenv("DB_PATH", "weather.db")
SOURCE_DB = os.getenv("SOURCE_DB", "source.db")
WTTR_URL  = os.getenv("WEATHER_API_URL", "https://wttr.in")
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "weather.duckdb")

API_FIELD_MAPPING = {
    "temp_C": "temp_c",
    "humidity": "humidity",
    "cloudcover": "cloudcover",
    "precipMM": "precipmm",
}

@dag(
    schedule="9 6 * * 7",  # 06:09 Sundays
    start_date=pendulum.datetime(2025, 8, 22, tz="UTC"),
    catchup=False,
    tags=["simple-days","sqlite","wttr","xorq"],
)
def weather_sqlite_wttr():
    @task(task_id="upstream-data-job")
    def upstream_data_job() -> list:
        """
        Fetch list of cities from the source database.
        """
        conn = sqlite3.connect(SOURCE_DB)
        cur = conn.cursor()
        cur.execute("SELECT city, country FROM cities")
        cities = [f"{city},{country}" for city, country in cur.fetchall()]
        conn.close()
        return cities

    @task()
    def enrich(cities: list, ds: str):
        """
        Enrich by fetching raw weather data from WTTR API and storing as-is.
        This task should never fail due to field name changes.
        """
        rows: list[dict] = []

        for city in cities:
            url = f"{WTTR_URL}/{quote_plus(city)}?format=j1"
            try:
                r = requests.get(url, timeout=15)
                r.raise_for_status()
                j = r.json()

                # Store the raw API response with minimal processing
                # Just add our metadata and let transform handle field extraction
                row = {
                    "dt": ds,
                    "city": city,
                    "timestamp": pd.Timestamp.utcnow().isoformat(),
                    "raw_api_response": json.dumps(j),  # Store entire response
                }

                # Also flatten current_condition for easier access in transform
                if "current_condition" in j and len(j["current_condition"]) > 0:
                    current = j["current_condition"][0]
                    # Add all current_condition fields with raw_ prefix to avoid conflicts
                    for key, value in current.items():
                        row[f"raw_{key}"] = value

                rows.append(row)
                logging.info(f"Fetched weather for {city} - fields: {list(current.keys()) if 'current' in locals() else 'none'}")

            except Exception as e:
                logging.error(f"Failed to fetch weather for {city}: {str(e)}")
                # Continue with other cities instead of failing the entire task
                continue

        if not rows:
            logging.warning(f"No weather data fetched for date {ds}")
            return

        df = pd.DataFrame(rows)
        con = duckdb.connect(DUCKDB_PATH)
        con.execute("CREATE SCHEMA IF NOT EXISTS staging")
        con.register("df", df)
        con.execute("CREATE OR REPLACE TABLE staging.weather_raw AS SELECT * FROM df")

        # Log what fields we captured for debugging
        columns = df.columns.tolist()
        raw_fields = [col for col in columns if col.startswith('raw_')]
        logging.info(f"Stored raw data with fields: {raw_fields}")

        con.close()

    @task()
    def transform(ds: str):
        """
        Transform staging data into the weather_model table.
        This task handles field mapping and will fail with clear errors if API changes.
        """
        con = duckdb.connect(DUCKDB_PATH)
        con.execute("CREATE SCHEMA IF NOT EXISTS main")
        con.execute("""
        CREATE TABLE IF NOT EXISTS main.weather_model (
            dt TEXT,
            city TEXT,
            temp_c DOUBLE,
            humidity DOUBLE,
            cloudcover DOUBLE,
            precipmm DOUBLE,
            rain_score DOUBLE,
            predict_rain BOOLEAN
        )
        """)

        # First, check what fields are available in the raw data
        available_fields = con.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'staging'
        AND table_name = 'weather_raw'
        AND column_name LIKE 'raw_%'
        """).fetchall()

        available_fields = [field[0] for field in available_fields]
        logging.info(f"Available raw fields: {available_fields}")

        field_mappings = []
        missing_fields = []

        for target_field in ["temp_c", "humidity", "cloudcover", "precipmm"]:
            mapped_field = None

            # Try to find the field using our mapping configuration
            for api_field, mapped_to in API_FIELD_MAPPING.items():
                if mapped_to == target_field and f"raw_{api_field}" in available_fields:
                    mapped_field = f"raw_{api_field}"
                    break

            if mapped_field:
                if target_field in ["temp_c", "precipmm"]:
                    # Ensure numeric fields are properly cast
                    field_mappings.append(f"CAST({mapped_field} AS DOUBLE) AS {target_field}")
                else:
                    field_mappings.append(f"CAST({mapped_field} AS DOUBLE) AS {target_field}")
            else:
                missing_fields.append(target_field)
                # Provide a fallback value or make it explicit
                field_mappings.append(f"NULL AS {target_field}")

        if missing_fields:
            available_raw = [f.replace('raw_', '') for f in available_fields]
            logging.error(f"Missing required fields: {missing_fields}")
            logging.error(f"Available API fields: {available_raw}")
            logging.error(f"Current field mapping config: {API_FIELD_MAPPING}")
            raise ValueError(
                f"Cannot map required fields {missing_fields} from available API fields {available_raw}. "
                f"Please update API_FIELD_MAPPING configuration."
            )

        # Remove any existing rows for this date, then insert fresh
        con.execute("DELETE FROM main.weather_model WHERE dt = ?", [ds])

        select_clause = ",\n                ".join([
            "dt",
            "city"
        ] + field_mappings + [
            "(humidity * cloudcover) / 10000.0 AS rain_score",
            "((humidity * cloudcover) / 10000.0 > 0.5) AS predict_rain"
        ])

        transform_query = f"""
            INSERT INTO main.weather_model
            SELECT
                {select_clause}
            FROM staging.weather_raw
            WHERE dt = ?
        """

        logging.info(f"Transform query: {transform_query}")
        con.execute(transform_query, [ds])

        row_count = con.execute(
            "SELECT COUNT(*) FROM main.weather_model WHERE dt = ?",
            [ds],
        ).fetchone()[0]

        logging.info(f"✅ Wrote main.weather_model rows={row_count} to {DUCKDB_PATH}")
        con.close()

    cities = upstream_data_job()
    enriched = enrich(cities, ds="{{ ds }}")
    transformed = transform(ds="{{ ds }}")
    enriched >> transformed

dag = weather_sqlite_wttr()
