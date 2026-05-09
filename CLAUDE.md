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
dbt run --select stg_netflix_titles int_netflix_titles_enriched int_netflix_genres_exploded mart_titles_by_country mart_titles_by_genre
dbt test --select stg_netflix_titles int_netflix_titles_enriched int_netflix_genres_exploded mart_titles_by_country mart_titles_by_genre
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
- Three-layer architecture: `staging → intermediate → marts`.
- `staging/stg_netflix_titles` (view): casts and trims raw seed data; filters null titles.
- `intermediate/int_netflix_titles_enriched` (view): adds `country_normalized` (first country, Unknown if blank), `decade`, `is_recent` (release_year ≥ 2015), `genre_count`, `date_added_year`. One row per title.
- `intermediate/int_netflix_genres_exploded` (view): unnests `listed_in` → one row per (title, genre) pair via DuckDB `unnest(string_split(...))`.
- `marts/mart_titles_by_country` (table): country-level aggregates sourced from `int_netflix_titles_enriched`; includes `recent_titles_count` and `avg_genre_count`.
- `marts/mart_titles_by_genre` (table): genre-level aggregates sourced from `int_netflix_genres_exploded`.
- Schema docs split by layer: `models/staging/schema.yml`, `models/intermediate/schema.yml`, `models/marts/schema.yml`. Seeds documented in `models/schema.yml`.
- Singular tests: `test_stg_release_year_range`, `test_int_decade_multiple`, `test_int_genre_row_count`, `test_mart_country_counts_consistent`, `test_mart_genre_counts_consistent`.
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