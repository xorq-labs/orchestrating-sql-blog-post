# dags/weather_pipeline.py
from airflow.decorators import dag, task
import pendulum
import os
import sqlite3
import requests
from urllib.parse import quote_plus

DB_PATH = os.getenv("DB_PATH", "weather.db")
CITIES   = os.getenv("WEATHER_CITIES", "Norfolk,US|New York,US|Chicago,US").split("|")
WTTR_URL = os.getenv("WEATHER_API_URL", "https://wttr.in")

def _ensure_tables(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS weather_daily(
      dt TEXT NOT NULL,
      city TEXT NOT NULL,
      temp_c REAL,
      weather TEXT,
      PRIMARY KEY (dt, city)
    );""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS weather_city_7d(
      dt TEXT NOT NULL,
      city TEXT NOT NULL,
      avg_temp_c_7d REAL,
      days INTEGER,
      dominant_weather TEXT,
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
    def fetch_and_load(ds: str):
        os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        _ensure_tables(cur)

        for city in CITIES:
            url = f"{WTTR_URL}/{quote_plus(city)}?format=j1"
            try:
                r = requests.get(url, timeout=15)
                if not r.ok:
                    continue
                j = r.json()
                temp_c = float(j["current_condition"][0]["temp_C"])
                weather = j["current_condition"][0]["weatherDesc"][0]["value"]
                cur.execute(
                    "INSERT OR REPLACE INTO weather_daily(dt,city,temp_c,weather) VALUES (?,?,?,?)",
                    (ds, city, temp_c, weather),
                )
            except Exception:
                # keep it tolerant in the "simple days" spirit
                continue

        conn.commit()
        conn.close()

    @task()
    def transform(ds: str):
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        _ensure_tables(cur)

        cur.execute("DELETE FROM weather_city_7d WHERE dt = ?", (ds,))
        cur.execute(
            """
            INSERT INTO weather_city_7d (dt, city, avg_temp_c_7d, days, dominant_weather)
            SELECT
              ?            AS dt,
              w.city       AS city,
              AVG(w.temp_c) AS avg_temp_c_7d,
              COUNT(*)      AS days,
              (
                SELECT wd.weather
                FROM weather_daily wd
                WHERE wd.city = w.city
                  AND wd.dt BETWEEN date(?,'-6 day') AND date(?)
                GROUP BY wd.weather
                ORDER BY COUNT(*) DESC, wd.weather
                LIMIT 1
              ) AS dominant_weather
            FROM weather_daily w
            WHERE w.dt BETWEEN date(?,'-6 day') AND date(?)
            GROUP BY w.city;
            """,
            (ds, ds, ds, ds, ds),
        )
        conn.commit()
        conn.close()

    # Pass ds via templated args (no get_current_context needed)
    fetch_and_load(ds="{{ ds }}") >> transform(ds="{{ ds }}")

dag = weather_sqlite_wttr()
