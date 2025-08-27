# WTTR.IN Stub Service

This stub service simulates the WTTR.IN weather API, allowing you to test your pipelines against a fixed schema response.

## Files

- `stub/wttr/app.py`: Flask application that serves the stub API.
- `stub/wttr/data/j1_basic.json`: Sample JSON response for `?format=j1`.

## Usage

1. Install dependencies:
   ```bash
   pip install flask
   ```

2. Start the stub server:
   ```bash
   python stub/wttr/app.py --host 0.0.0.0 --port 5000 --data-file stub/wttr/data/j1_basic.json
   ```

3. Point your pipelines to the stub service by setting the `WEATHER_API_URL` environment variable. For example:
   ```bash
   export WEATHER_API_URL="http://localhost:5000"
   ```

4. Run your pipelines (bash, Airflow, Dagster, expr plan):
- **Bash script**:
  ```bash
  bash 01-cron-bash/weather_daily.sh
  ```
- **Airflow DAG**: Configure `WEATHER_API_URL` in your Airflow environment.
- **Dagster assets**: Ensure `WEATHER_API_URL` is set in your Dagster run environment.
- **Expr plan**: Use `WEATHER_API_URL` when running `04-expr-plan/expr.py`.

## Testing Schema Changes

To simulate schema changes, modify the `stub/wttr/data/j1_basic.json` file:

- Remove or rename fields to test missing field handling.
- Add new fields to test schema evolution.

This will help you observe how your pipelines react to changes in the WTTR.IN response schema.