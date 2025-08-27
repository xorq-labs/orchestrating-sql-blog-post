#!/usr/bin/env bash
set -euo pipefail
DB="${DB:-weather.db}"
RUN_DATE="${1:-$(date -u +%F)}"                     # e.g., 2025-08-23

# Build CITIES list from source.db (table 'cities' with city,country columns)
SOURCE_DB="source.db"

# Ensure source DB has a cities table to drive the script
if ! sqlite3 "$SOURCE_DB" "SELECT name FROM sqlite_master WHERE type='table' AND name='cities';" \
      | grep -q cities; then
  cat <<EOF >&2
Error: 'cities' table not found in $SOURCE_DB.
Please create it, for example:
  sqlite3 "$SOURCE_DB" <<SQL
    CREATE TABLE cities(
      city TEXT NOT NULL,
      country TEXT NOT NULL,
      PRIMARY KEY(city,country)
    );
    INSERT INTO cities VALUES('Norfolk','US'),('New York','US'),('Chicago','US');
  SQL
EOF
  exit 1
fi

CITIES="$(sqlite3 "$SOURCE_DB" -noheader \
  "SELECT city||','||country FROM cities;" \
  | tr '\n' '|' | sed 's/|$//')"

TMP="$(mktemp)"; trap 'rm -f "$TMP"' EXIT

# Iterate over cities defined in CITIES (pipe-separated), preserving spaces
IFS='|' read -r -a CITY_ARR <<< "$CITIES"
for CITY in "${CITY_ARR[@]}"; do
  # Fetch current conditions from wttr.in and extract required fields
  data=$(curl -s "https://wttr.in/${CITY// /+}?format=j1")
  temp_c=$(echo "$data" | jq -r '.current_condition[0].temp_C | tonumber')
  humidity=$(echo "$data" | jq -r '.current_condition[0].humidity | tonumber')
  cloud=$(echo "$data" | jq -r '.current_condition[0].cloudcover | tonumber')
  precip=$(echo "$data" | jq -r '.current_condition[0].precipMM | tonumber')
  # Emit CSV: date, city, temp, humidity, cloudcover, precip
  printf '"%s","%s",%.1f,%.1f,%.1f,%.2f\n' \
    "$RUN_DATE" "$CITY" "$temp_c" "$humidity" "$cloud" "$precip" \
    >> "$TMP"
done

sqlite3 "$DB" <<SQL
-- Create and populate modeled table with rain_score and prediction
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
);
-- Staging import for this run
DROP TABLE IF EXISTS _stg_weather;
CREATE TEMP TABLE _stg_weather(
  dt TEXT, city TEXT, temp_c REAL, humidity REAL,
  cloudcover REAL, precipmm REAL
);
.mode csv
.import $TMP _stg_weather
BEGIN;
-- Replace any existing rows for this date
DELETE FROM weather_model WHERE dt = '$RUN_DATE';
-- Compute rain_score and predict_rain, then load
INSERT INTO weather_model
SELECT
  dt, city, temp_c, humidity, cloudcover, precipmm,
  (humidity * cloudcover)/10000.0 AS rain_score,
  ((humidity * cloudcover)/10000.0 > 0.5) AS predict_rain
FROM _stg_weather;
COMMIT;
DROP TABLE _stg_weather;
SQL

echo "loaded $(wc -l < "$TMP") rows into $DB:weather_model for $RUN_DATE"

echo
echo "=== weather_model for $RUN_DATE ==="
sqlite3 -header -column "$DB" "SELECT * FROM weather_model WHERE dt = '$RUN_DATE';"
