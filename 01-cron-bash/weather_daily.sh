#!/usr/bin/env bash
set -euo pipefail
DB="${DB:-weather.db}"
RUN_DATE="${1:-$(date -u +%F)}"                     # e.g., 2025-08-23
CITIES="${CITIES:-Norfolk,US|New York,US|Chicago,US}"

TMP="$(mktemp)"; trap 'rm -f "$TMP"' EXIT

for CITY in ${CITIES//|/ } ; do
  curl -s "https://wttr.in/${CITY// /+}?format=j1" \
  | jq -r --arg d "$RUN_DATE" --arg city "$CITY" \
      '[ $d, $city, (.current_condition[0].temp_C|tonumber), .current_condition[0].weatherDesc[0].value ] | @csv' \
  >> "$TMP"
done

sqlite3 "$DB" <<SQL
-- Target tables
CREATE TABLE IF NOT EXISTS weather_daily(
  dt     TEXT NOT NULL,
  city   TEXT NOT NULL,
  temp_c REAL,
  weather TEXT,
  PRIMARY KEY (dt, city)
);

CREATE TABLE IF NOT EXISTS weather_city_7d(
  dt TEXT NOT NULL,
  city TEXT NOT NULL,
  avg_temp_c_7d REAL,
  days INTEGER,
  dominant_weather TEXT,
  PRIMARY KEY (dt, city)
);

-- Staging import
DROP TABLE IF EXISTS _stg_weather;
CREATE TEMP TABLE _stg_weather(dt TEXT, city TEXT, temp_c REAL, weather TEXT);
.mode csv
.import $TMP _stg_weather

BEGIN;

-- Upsert into daily table (old-SQLite compatible)
UPDATE weather_daily
SET temp_c = (SELECT s.temp_c  FROM _stg_weather s WHERE s.dt = weather_daily.dt AND s.city = weather_daily.city),
    weather = (SELECT s.weather FROM _stg_weather s WHERE s.dt = weather_daily.dt AND s.city = weather_daily.city)
WHERE EXISTS (SELECT 1 FROM _stg_weather s WHERE s.dt = weather_daily.dt AND s.city = weather_daily.city);

INSERT INTO weather_daily(dt, city, temp_c, weather)
SELECT s.dt, s.city, s.temp_c, s.weather
FROM _stg_weather s
WHERE NOT EXISTS (SELECT 1 FROM weather_daily t WHERE t.dt = s.dt AND t.city = s.city);
COMMIT;
DROP TABLE _stg_weather;
SQL

sqlite3 "$DB" <<SQL
-- Transform: rebuild 7-day aggregate for RUN_DATE
BEGIN;
DELETE FROM weather_city_7d WHERE dt = '$RUN_DATE';

INSERT INTO weather_city_7d (dt, city, avg_temp_c_7d, days, dominant_weather)
SELECT
  '$RUN_DATE' AS dt,
  w.city,
  AVG(w.temp_c) AS avg_temp_c_7d,
  COUNT(*)      AS days,
  (
    SELECT wd.weather
    FROM weather_daily wd
    WHERE wd.city = w.city
      AND wd.dt BETWEEN date('$RUN_DATE','-6 day') AND date('$RUN_DATE')
    GROUP BY wd.weather
    ORDER BY COUNT(*) DESC, wd.weather
    LIMIT 1
  ) AS dominant_weather
FROM weather_daily w
WHERE w.dt BETWEEN date('$RUN_DATE','-6 day') AND date('$RUN_DATE')
GROUP BY w.city;
COMMIT;

SQL

echo "loaded \$(wc -l < "$TMP") rows into $DB:weather_daily and rebuilt $DB:weather_city_7d for $RUN_DATE"
