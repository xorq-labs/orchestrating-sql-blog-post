# Airflow DAG Example

This folder demonstrates the **General-Purpose Orchestrator** era of SQL orchestration, using Apache Airflow to manage scheduling, retries, and task dependencies.

## Files

- `dags/weather_daily_simple.py`: an Airflow DAG


## Usage

1. Ensure the WTTR.IN stub service is running (see `stub/WTTR_STUB_SERVICE.md`):

   ```bash
   WEATHER_API_URL=http://localhost:5002
   ```
   optionally, run the stub service:

   ```python
      python stub/wttr/app.py --host 0.0.0.0 --port 5002 --data-file stub/wttr/data/j1_basic.json
    ```

2. Run Airflow in standalone mode, pointing to this folder's DAGs and the stub service:

   ```bash
   AIRFLOW__CORE__DAGS_FOLDER=02-airflow/dags \
   WEATHER_API_URL=http://localhost:5002 \
   airflow standalone
   ```
