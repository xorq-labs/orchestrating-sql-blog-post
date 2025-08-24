import os, time, pandas as pd, requests, duckdb
from urllib.parse import quote_plus
from dagster import asset, Definitions, define_asset_job, ScheduleDefinition
from dagster_dbt import DbtCliResource

WTTR_URL    = os.getenv("WEATHER_API_URL", "https://wttr.in")
CITIES      = [c.strip() for c in os.getenv(
    "WEATHER_CITIES", "Norfolk,US|New York,US|Chicago,US"
).split("|") if c.strip()]

import os, pathlib
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "weather.duckdb")


@asset
def weather_now(context):
    rows = []
    now_iso = pd.Timestamp.utcnow().isoformat()
    for city in CITIES:
        r = requests.get(f"{WTTR_URL}/{quote_plus(city)}", params={"format":"j1"}, timeout=15)
        r.raise_for_status()
        j = r.json()
        rows.append({
            "city": city,
            "timestamp": now_iso,
            "temp_c": float(j["current_condition"][0]["temp_C"]),
            "weather": j["current_condition"][0]["weatherDesc"][0]["value"],
        })
    df = pd.DataFrame(rows, columns=["city","timestamp","temp_c","weather"])

    os.makedirs(pathlib.Path(DUCKDB_PATH).parent, exist_ok=True)
    con = duckdb.connect(DUCKDB_PATH)
    con.register("df", df)
    con.execute("CREATE SCHEMA IF NOT EXISTS main;")
    con.execute("CREATE OR REPLACE TABLE main.weather_now AS SELECT * FROM df;")
    n = con.execute("SELECT COUNT(*) FROM main.weather_now;").fetchone()[0]
    con.close()
    context.log.info(f"✅ Wrote main.weather_now rows={n} to {DUCKDB_PATH}")


@asset(deps=[weather_now], required_resource_keys={"dbt"})
def build_weather_city_avg(context):
    # just run dbt; no manifest-bound translation needed
    context.resources.dbt.cli(
        ["build", "--select", "weather_city_avg"]
    ).wait()


# 06:09 every Sunday
job = define_asset_job("weather_job", selection=[build_weather_city_avg])
schedule = ScheduleDefinition(job=job, cron_schedule="9 6 * * 7")

defs = Definitions(
    assets=[weather_now, build_weather_city_avg],
    resources={
        "dbt": DbtCliResource(
            project_dir=os.getenv("DBT_PROJECT_DIR", "./dbt_weather"),
            profiles_dir=os.getenv("DBT_PROFILES_DIR", os.path.expanduser("~/.dbt")),
        )
    },
    schedules=[schedule],
)
