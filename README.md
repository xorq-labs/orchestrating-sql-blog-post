# Orchestrating SQL Blog Post Examples

This repository contains the code examples that accompany the _Orchestrating SQL_ blog post. It demonstrates four eras of SQL orchestration:

- **Cron + Bash**: simple shell scripts with cron scheduling (01-cron-bash)
- **General-Purpose Orchestrator**: an Apache Airflow DAG example (02-airflow)
- **Asset-Based Frameworks**: dbt models orchestrated by Dagster assets (03-assets-dbt-dagster)
- **Expression-Based Execution**: a unified plan using Xorq expressions (04-expr-plan)

## Repository Structure

```text
01-cron-bash/          # Bash script and cron example
02-airflow/            # Airflow DAG example
03-assets-dbt-dagster/  # Dagster assets and dbt project example
04-expr-plan/          # Xorq expression plan example
stub/                  # WTTR.IN stub service for testing pipelines
```
