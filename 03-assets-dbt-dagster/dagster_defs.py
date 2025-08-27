import os
from pathlib import Path
import pandas as pd
import requests
import duckdb
import sqlite3
from urllib.parse import quote_plus
import json
import logging
from dagster import asset, Definitions, define_asset_job, ScheduleDefinition
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets, get_asset_key_for_source

WTTR_URL = os.getenv("WEATHER_API_URL", "https://wttr.in")
API_FIELD_MAPPING = {
    "temp_C": "temp_c",
    "humidity": "humidity",
    "cloudcover": "cloudcover",
    "precipMM": "precipmm",
}

# local dbt project and DuckDB file (shared between Python asset & dbt)
DBT_PROJECT_DIR = Path(__file__).parent / "dbt_weather"
dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=Path(os.getenv("DBT_PROFILES_DIR", "~/.dbt")).expanduser(),
)
dbt_project.prepare_if_dev()

DUCKDB_PATH = os.getenv("DUCKDB_PATH", str(DBT_PROJECT_DIR / "weather.duckdb"))

@asset(name="cities")
def fetch_cities() -> list[str]:
    """Fetch list of cities from the source database."""
    conn = sqlite3.connect(os.getenv("SOURCE_DB", "source.db"))
    cur = conn.cursor()
    cur.execute("SELECT city, country FROM cities")
    cities = [f"{city},{country}" for city, country in cur.fetchall()]
    conn.close()
    return cities


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
)
def weather_dbt_assets(context, dbt: DbtCliResource):
    """
    Build the dbt model weather_city_avg from the weather_now source.
    Also fetch row counts and column metadata for UI visibility.
    """
    yield from (
        dbt.cli(["build", "--select", "weather_city_avg"], context=context)
        .stream()
        .fetch_row_counts()
        .fetch_column_metadata()
    )



@asset(key=get_asset_key_for_source([weather_dbt_assets], "main"))
def weather_now(context, cities: list[str]):
    """
    Enrich by fetching raw weather data from WTTR API, apply field mappings,
    and write to the main.weather_now table for downstream dbt models.
    """
    rows: list[dict] = []
    now_ts = pd.Timestamp.utcnow().isoformat()

    for city in cities:
        url = f"{WTTR_URL}/{quote_plus(city)}?format=j1"
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        j = r.json()

        flat = {f"raw_{k}": v for k, v in j.get("current_condition", [{}])[0].items()}

        row = {"city": city, **flat}
        rows.append(row)
        logging.info(f"Fetched weather for {city} - fields: {list(flat.keys())}")

    df = pd.DataFrame(rows)

    for api_field, target in API_FIELD_MAPPING.items():
        df[target] = pd.to_numeric(df.get(f"raw_{api_field}"), errors="coerce")

    con = duckdb.connect(DUCKDB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS main")
    con.register("df", df)
    con.execute(
        "CREATE OR REPLACE TABLE main.weather_staging AS SELECT city,"
        + ", ".join(list(API_FIELD_MAPPING.values()))
        + ", FROM df"
    )
    count = con.execute("SELECT COUNT(*) FROM main.weather_staging").fetchone()[0]
    con.close()
    context.log.info(f"✅ Wrote main.weather_staging rows={count} to {DUCKDB_PATH}")


# 06:09 every Sunday
job = define_asset_job(
    "weather_job",
    selection=[fetch_cities, weather_now, weather_dbt_assets],
)
schedule = ScheduleDefinition(job=job, cron_schedule="9 6 * * 7")

defs = Definitions(
    assets=[fetch_cities, weather_now, weather_dbt_assets],
    resources={
        "dbt": DbtCliResource(
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=os.getenv("DBT_PROFILES_DIR", os.path.expanduser("~/.dbt")),
        )
    },
    schedules=[schedule],
)
