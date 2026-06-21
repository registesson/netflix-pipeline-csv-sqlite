"""
Tests de structure du DAG Airflow.

Vérifie le graphe de tâches, les dépendances, les configurations de retry/timeout
et les comportements spéciaux — sans exécuter de run Airflow.
"""

from datetime import timedelta

import pytest

pytest.importorskip("airflow")
from airflow.utils.trigger_rule import TriggerRule

import netflix_pipeline_dag as dag_mod

dag = dag_mod.dag


# ── Propriétés du DAG ────────────────────────────────────────────────────────

def test_dag_loads():
    assert dag is not None
    assert dag.dag_id == "netflix_pipeline"


def test_dag_does_not_catchup():
    assert dag.catchup is False


def test_dag_tags():
    assert {"netflix", "etl", "sqlite"}.issubset(set(dag.tags))


def test_dag_has_expected_tasks():
    expected = {
        "load_csv", "clean_data", "insert_to_db", "generate_report",
        "summary", "copy_to_dbt_seed", "dbt_seed", "dbt_snapshot", "dbt_run",
        "notify_slack",
    }
    assert set(dag.task_ids) == expected


# ── Dépendances ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize("task_id,expected_downstream", [
    ("load_csv",         {"clean_data"}),
    ("clean_data",       {"insert_to_db", "copy_to_dbt_seed"}),   # bifurcation
    ("insert_to_db",     {"generate_report"}),
    ("generate_report",  {"summary"}),
    ("copy_to_dbt_seed", {"dbt_seed"}),
    ("dbt_seed",         {"dbt_snapshot"}),
    ("dbt_snapshot",     {"dbt_run"}),
    ("dbt_run",          {"summary"}),                             # convergence
    ("summary",          {"notify_slack"}),
    ("notify_slack",     set()),
])
def test_task_downstream(task_id, expected_downstream):
    assert dag.get_task(task_id).downstream_task_ids == expected_downstream


# ── Retry et timeout par tâche ───────────────────────────────────────────────

@pytest.mark.parametrize("task_id,retries,retry_delay,timeout", [
    ("load_csv",         2, timedelta(seconds=30), timedelta(minutes=5)),
    ("clean_data",       1, timedelta(minutes=1),  timedelta(minutes=10)),
    ("insert_to_db",     3, timedelta(minutes=2),  timedelta(minutes=10)),
    ("generate_report",  2, timedelta(minutes=1),  timedelta(minutes=5)),
    ("copy_to_dbt_seed", 2, timedelta(seconds=30), timedelta(minutes=3)),
    ("dbt_seed",         2, timedelta(minutes=3),  timedelta(minutes=15)),
    ("dbt_snapshot",     2, timedelta(minutes=3),  timedelta(minutes=15)),
    ("dbt_run",          2, timedelta(minutes=3),  timedelta(minutes=15)),
    ("summary",          0, None,                  timedelta(minutes=2)),
    ("notify_slack",     1, timedelta(seconds=30), timedelta(minutes=2)),
])
def test_task_retry_and_timeout(task_id, retries, retry_delay, timeout):
    task = dag.get_task(task_id)
    assert task.retries == retries, f"{task_id}: retries incorrect"
    assert task.execution_timeout == timeout, f"{task_id}: execution_timeout incorrect"
    if retry_delay is not None:
        assert task.retry_delay == retry_delay, f"{task_id}: retry_delay incorrect"


# ── Comportements spécifiques ─────────────────────────────────────────────────

def test_insert_to_db_has_exponential_backoff():
    assert dag.get_task("insert_to_db").retry_exponential_backoff is True


def test_other_tasks_have_no_exponential_backoff():
    for task in dag.tasks:
        if task.task_id == "insert_to_db":
            continue
        assert not task.retry_exponential_backoff, (
            f"retry_exponential_backoff inattendu sur '{task.task_id}'"
        )


@pytest.mark.parametrize("task_id", ["summary", "notify_slack"])
def test_all_done_trigger_rule(task_id):
    assert dag.get_task(task_id).trigger_rule == TriggerRule.ALL_DONE


def test_other_tasks_have_default_trigger_rule():
    for task in dag.tasks:
        if task.task_id in ("summary", "notify_slack"):
            continue
        assert task.trigger_rule == TriggerRule.ALL_SUCCESS, (
            f"trigger_rule inattendu sur '{task.task_id}'"
        )


def test_all_tasks_have_failure_callback():
    for task in dag.tasks:
        assert task.on_failure_callback, (
            f"on_failure_callback manquant sur '{task.task_id}'"
        )


def test_failure_callback_is_correct_function():
    for task in dag.tasks:
        assert dag_mod.on_task_failure in task.on_failure_callback, (
            f"on_task_failure absent du callback de '{task.task_id}'"
        )
