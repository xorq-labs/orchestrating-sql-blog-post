# dags/weather_pipeline.py
from airflow.decorators import dag, task
import pendulum
import os
import sqlite3
import requests
from urllib.parse import quote_plus

DB_PATH   = os.getenv("DB_PATH", "weather.db")
SOURCE_DB = os.getenv("SOURCE_DB", "source.db")
WTTR_URL  = os.getenv("WEATHER_API_URL", "https://wttr.in")


def _ensure_tables(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS weather_raw(
      dt TEXT NOT NULL,
      city TEXT NOT NULL,
      temp_c REAL,
      humidity REAL,
      cloudcover REAL,
      precipmm REAL,
      PRIMARY KEY (dt, city)
    );""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS weather_model(
      dt TEXT NOT NULL,
      city TEXT NOT NULL,
      temp_c REAL,
      humidity REAL,
      cloudcover REAL,
      precipmm REAL,
      rain_score REAL,
      predict_rain INTEGER,
      PRIMARY KEY (dt, city)
    );""")

@dag(
    schedule="9 6 * * 7",  # 06:09 Sundays (XORQ)
    start_date=pendulum.datetime(2025, 8, 22, tz="UTC"),
    catchup=False,
    tags=["simple-days","sqlite","wttr","xorq"],
)
def weather_sqlite_wttr():
    @task()
    def fetch_from_source() -> list:
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
        Enrich raw weather data by querying the WTTR API and loading into raw table.
        """
        os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        _ensure_tables(cur)

        for city in cities:
            url = f"{WTTR_URL}/{quote_plus(city)}?format=j1"
            try:
                r = requests.get(url, timeout=15)
                if not r.ok:
                    continue
                j = r.json()
                temp_c = float(j["current_condition"][0]["temp_C"])
                humidity = float(j["current_condition"][0]["humidity"])
                cloud = float(j["current_condition"][0]["cloudcover"])
                precip = float(j["current_condition"][0]["precipMM"])
                cur.execute(
                    "INSERT OR REPLACE INTO weather_raw(dt,city,temp_c,humidity,cloudcover,precipmm)"
                    " VALUES (?,?,?,?,?,?)",
                    (ds, city, temp_c, humidity, cloud, precip),
                )
            except Exception:
                continue

        conn.commit()
        conn.close()

    @task()
    def transform(ds: str):
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        _ensure_tables(cur)

        cur.execute("DELETE FROM weather_model WHERE dt = ?", (ds,))
        cur.execute(
            """
            INSERT INTO weather_model(dt,city,temp_c,humidity,cloudcover,precipmm,rain_score,predict_rain)
            SELECT
              dt,
              city,
              temp_c,
              humidity,
              cloudcover,
              precipmm,
              (humidity*cloudcover)/10000.0        AS rain_score,
              ((humidity*cloudcover)/10000.0 > 0.5) AS predict_rain
            FROM weather_raw
            WHERE dt = ?
            """,
            (ds,),
        )
        conn.commit()
        conn.close()

    cities = fetch_from_source()
    enriched = enrich(cities, ds="{{ ds }}")
    transformed = transform(ds="{{ ds }}")
    enriched >> transformed

dag = weather_sqlite_wttr()
