# Cron + Bash Example

This folder demonstrates the **Cron + Bash** era of SQL orchestration. A simple shell script fetches weather data from the WTTR.IN API and loads it into Postgres.

## Files

- `weather_daily.sh`: Bash script that queries the WTTR.IN API, formats the CSV output, and loads it into `dm.weather_daily` via `psql`.

## Prerequisites

- `bash`, `curl`, `jq`, and the `psql` or `sqllite` client must be installed and available in your `PATH`.
- Optionally point to the stub service by setting `WEATHER_API_URL`.

## Usage

1. Ensure `WEATHER_API_URL` and `OPENWEATHER_API_KEY` are exported:

   ```bash
   export WEATHER_API_URL=http://localhost:5000 # or omit it to use http://wttr.in
   ```

2. Run the script (defaults to today's date if no argument is provided):

   ```bash
   bash weather_daily.sh
   ```

3. To schedule via cron (as in the blog post):

   ```cron
   9 6 * * 7 /path/to/01-cron-bash/weather_daily.sh >> /var/log/events_daily.log 2>&1
   ```
