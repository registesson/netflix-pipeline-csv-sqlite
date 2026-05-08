# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run the full pipeline
python main.py

# Run with options
python main.py --show-examples
python main.py --if-exists append
python main.py --input data/netflix_titles.csv --db data/netflix.db --report outputs/report.json

# Run tests
pytest
pytest tests/test_clean.py          # single test file
pytest tests/test_clean.py::test_clean_data_basic  # single test

# dbt (local, from repo root)
./scripts/run_dbt_local.sh          # runs pipeline → dbt seed/run/test

# Or step by step:
cd dbt && export DBT_PROFILES_DIR=$(pwd)
dbt seed --full-refresh
dbt run --select stg_netflix_titles mart_titles_by_country
dbt test --select stg_netflix_titles mart_titles_by_country
```

## Architecture

This project is a learning-oriented ETL pipeline for Netflix titles data with three execution paths sharing the same `pipeline/` modules:

**1. Core Python pipeline** (`main.py` + `pipeline/`)
- `loader.py` → `clean.py` → `db.py` → `report.py`
- Input: `data/netflix_titles.csv`, Output: `data/netflix.db` (SQLite) + `outputs/`
- Schema validation is done via `pandera` in `clean.py` — errors are logged as warnings (non-fatal) so the pipeline continues with best-effort data.
- `report.py` generates both `report.json` and `report.html` from the same call; the HTML template is at `pipeline/templates/report.html.j2`.

**2. Airflow DAG** (`dags/netflix_pipeline_dag.py`)
- Orchestrates the same pipeline steps as `main.py` plus a parallel dbt branch.
- Task graph: `load_csv >> clean_data >> insert_to_db >> generate_report >> summary >> notify_slack` and `clean_data >> copy_to_dbt_seed >> dbt_seed >> dbt_snapshot >> dbt_run >> summary`.
- `notify_slack` runs with `TriggerRule.ALL_DONE` and posts pipeline metrics (rows loaded/cleaned/dropped, report path) to Slack via `SLACK_WEBHOOK_URL`. Silently skipped if the variable is unset. Status is inferred from XComs (no `dag_run.get_task_instances()` — removed in Airflow 3.x).
- All paths are resolved relative to `PROJECT_ROOT` (parent of `dags/`) so the DAG works regardless of `AIRFLOW_HOME`.
- Configured daily (`@daily`), `catchup=False`.

**3. dbt / DuckDB path** (`dbt/`)
- Reads from `dbt/seeds/netflix_titles.csv` (populated by `main.py --cleaned-output dbt/seeds/netflix_titles.csv` or the Airflow `copy_to_dbt_seed` task).
- `staging/stg_netflix_titles` (view): normalizes/casts seed data.
- `marts/mart_titles_by_country` (table): country-level aggregates for reporting.
- Profile uses a local DuckDB file (`data/netflix.duckdb`); `DBT_PROFILES_DIR` must point to `dbt/` when running locally.

## Environment

Copy `.env.example` to `.env`. All five path variables (`INPUT_PATH`, `CLEANED_OUTPUT`, `DB_PATH`, `REPORT_PATH`, `IF_EXISTS`) are read by both `main.py` and the Airflow DAG; CLI flags override env vars. `SLACK_WEBHOOK_URL` is read only by the DAG; if unset, `notify_slack` logs a warning and exits cleanly.

## Airflow 3.x compatibility

- `dag_run.get_task_instances()` no longer exists in Airflow 3.x — do not use it in task callables.
- The `context["dag_run"]` object is a Pydantic model from the Task SDK; it exposes `dag_id`, `run_id`, `state`, and data interval fields, but no ORM methods.
- To infer pipeline status from within a task, use XCom values rather than querying task instance states.
- The `pipeline/` module must be mounted at `/opt/airflow/pipeline` in the Docker setup so `PROJECT_ROOT` (parent of `dags/`) resolves it correctly.

## Key data contract

The `clean_data()` function requires these columns to be present or raises `ValueError`: `title`, `date_added`, `release_year`, `country`, `type`. The pandera schema (`NETFLIX_SCHEMA` in `pipeline/clean.py`) validates types and value ranges after cleaning.