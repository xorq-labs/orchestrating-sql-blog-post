# dbt + Dagster Asset-Based Example

This folder demonstrates the **Asset-Based Frameworks** era of SQL orchestration, using dbt for SQL transformations and Dagster assets for orchestration.

## Files

- `dagster_defs.py`: Dagster definitions including:
  - `fetch_cities`: reads city list from SQLite
  - `weather_now`: enriches weather data via HTTP
  - `weather_dbt_assets`: builds the dbt model
- `dbt_weather/`: a starter dbt project with the `weather_city_avg` model


## Usage

1. (Optional) Ensure the WTTR.IN stub service is running (see `stub/WTTR_STUB_SERVICE.md`):

   ```bash
   WEATHER_API_URL=http://localhost:5002 bash stub/wttr/app.py --host 0.0.0.0 --port 5002 --data-file stub/wttr/data/j1_basic.json
   ```

2. Launch Dagster in development mode with these definitions:

   ```bash
   WEATHER_API_URL=http://localhost:5002 dagster dev -f 03-assets-dbt-dagster/dagster_defs.py
   ```

3. In the Dagster UI (http://localhost:3000), execute the `weather_job` or let the schedule run.

4. The dbt project lives in `03-assets-dbt-dagster/dbt_weather/`. See its own `README.md` for dbt-specific commands.
