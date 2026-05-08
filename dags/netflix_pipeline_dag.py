"""
DAG Airflow - Pipeline Netflix
CSV → Nettoyage → SQLite → Rapport JSON/HTML
                → dbt (DuckDB) seed + snapshot + run

Structure des tâches :
    load_csv >> clean_data >> insert_to_db     >> generate_report >> summary >> notify_slack
                           >> copy_to_dbt_seed >> dbt_seed >> dbt_snapshot >> dbt_run ──┘
"""

import os
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# ── Résolution du répertoire racine du projet ──────────────────────────────────
# Permet d'importer les modules du projet peu importe le répertoire de travail Airflow
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ── Chemins par défaut (surchargeables via variables Airflow ou env) ───────────
INPUT_PATH      = os.getenv("INPUT_PATH",      str(PROJECT_ROOT / "data"    / "netflix_titles.csv"))
CLEANED_OUTPUT  = os.getenv("CLEANED_OUTPUT",  str(PROJECT_ROOT / "outputs" / "cleaned_data.csv"))
DB_PATH         = os.getenv("DB_PATH",         str(PROJECT_ROOT / "data"    / "netflix.db"))
REPORT_PATH     = os.getenv("REPORT_PATH",     str(PROJECT_ROOT / "outputs" / "report.json"))
IF_EXISTS       = os.getenv("IF_EXISTS",       "replace")

DBT_DIR           = str(PROJECT_ROOT / "dbt")
DBT_SEED_PATH     = str(PROJECT_ROOT / "dbt" / "seeds" / "netflix_titles.csv")
DBT_BIN           = shutil.which("dbt") or "dbt"

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")

# ── Callback d'échec ──────────────────────────────────────────────────────────
def on_task_failure(context) -> None:
    ti = context["task_instance"]
    exception = context.get("exception")
    print(
        f"[FAILURE] DAG={ti.dag_id} | task={ti.task_id} | run={ti.run_id} | "
        f"try={ti.try_number}/{ti.max_tries + 1} | error={exception}"
    )


# ── Arguments par défaut du DAG ───────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_task_failure,
}

# ── Définition des callables (une fonction = une tâche) ───────────────────────

def task_load_csv(**context) -> None:
    """Charge le fichier CSV et vérifie qu'il est lisible."""
    from pipeline.loader import load_csv

    if not Path(INPUT_PATH).exists():
        raise FileNotFoundError(f"Fichier source introuvable : {INPUT_PATH}")

    try:
        df = load_csv(INPUT_PATH)
    except Exception as exc:
        raise RuntimeError(f"Impossible de lire {INPUT_PATH}") from exc

    if df.empty:
        raise ValueError(f"Le fichier CSV est vide : {INPUT_PATH}")

    context["ti"].xcom_push(key="row_count_raw", value=len(df))
    print(f"[load_csv] {len(df)} lignes chargées depuis {INPUT_PATH}")


def task_clean_data(**context) -> None:
    """Nettoie les données et les sauvegarde en CSV intermédiaire horodaté."""
    from pipeline.loader import load_csv
    from pipeline.clean import clean_data

    ds = context.get("ds") or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    base = Path(CLEANED_OUTPUT)
    cleaned_path = str(base.parent / f"{base.stem}_{ds}{base.suffix}")
    Path(cleaned_path).parent.mkdir(parents=True, exist_ok=True)

    df_raw = load_csv(INPUT_PATH)

    try:
        df_cleaned = clean_data(df_raw)
    except ValueError as exc:
        raise ValueError(f"Échec du nettoyage — colonnes manquantes ou invalides : {exc}") from exc
    except Exception as exc:
        raise RuntimeError(f"Erreur inattendue lors du nettoyage : {exc}") from exc

    if df_cleaned.empty:
        raise ValueError("Le DataFrame nettoyé est vide — abandon du pipeline.")

    df_cleaned.to_csv(cleaned_path, index=False)

    context["ti"].xcom_push(key="row_count_cleaned", value=len(df_cleaned))
    context["ti"].xcom_push(key="cleaned_output_path", value=cleaned_path)
    print(f"[clean_data] {len(df_cleaned)} lignes nettoyées → {cleaned_path}")


def task_insert_to_db(**context) -> None:
    """Insère les données nettoyées dans la base SQLite."""
    import pandas as pd
    from pipeline.db import insert_to_db

    cleaned_path = context["ti"].xcom_pull(task_ids="clean_data", key="cleaned_output_path")
    if not cleaned_path or not Path(cleaned_path).exists():
        raise FileNotFoundError(f"CSV nettoyé introuvable via XCom : {cleaned_path}")

    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

    df_cleaned = pd.read_csv(cleaned_path)

    try:
        insert_to_db(df_cleaned, DB_PATH, IF_EXISTS)
    except Exception as exc:
        # SQLite peut être verrouillé temporairement — le retry_exponential_backoff gère la contention
        raise RuntimeError(f"Échec de l'insertion dans {DB_PATH} : {exc}") from exc

    print(f"[insert_to_db] {len(df_cleaned)} lignes insérées dans {DB_PATH} (mode={IF_EXISTS})")


def task_generate_report(**context) -> None:
    """Génère le rapport JSON et HTML horodatés depuis les données nettoyées."""
    import pandas as pd
    from pipeline.report import generate_report

    ds = context.get("ds") or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    cleaned_path = context["ti"].xcom_pull(task_ids="clean_data", key="cleaned_output_path")
    if not cleaned_path or not Path(cleaned_path).exists():
        raise FileNotFoundError(f"CSV nettoyé introuvable via XCom : {cleaned_path}")

    base = Path(REPORT_PATH)
    report_path = str(base.parent / f"{base.stem}_{ds}{base.suffix}")
    Path(report_path).parent.mkdir(parents=True, exist_ok=True)

    df_cleaned = pd.read_csv(cleaned_path)

    try:
        generate_report(df_cleaned, report_path)
    except Exception as exc:
        raise RuntimeError(f"Échec de la génération du rapport vers {report_path} : {exc}") from exc

    context["ti"].xcom_push(key="report_path", value=report_path)
    print(f"[generate_report] Rapport généré → {report_path}")


def task_copy_to_dbt_seed(**context) -> None:
    """Copie le CSV nettoyé horodaté vers dbt/seeds/ pour alimenter le seed dbt."""
    import shutil
    cleaned_path = context["ti"].xcom_pull(task_ids="clean_data", key="cleaned_output_path")
    if not cleaned_path or not Path(cleaned_path).exists():
        raise FileNotFoundError(f"CSV nettoyé introuvable via XCom : {cleaned_path}")

    Path(DBT_SEED_PATH).parent.mkdir(parents=True, exist_ok=True)

    try:
        shutil.copy2(cleaned_path, DBT_SEED_PATH)
    except OSError as exc:
        raise RuntimeError(f"Échec de la copie vers {DBT_SEED_PATH} : {exc}") from exc

    print(f"[copy_to_dbt_seed] {cleaned_path} → {DBT_SEED_PATH}")


def task_summary(**context) -> None:
    """Affiche un résumé des métriques du pipeline via XCom."""
    ti = context["ti"]
    raw          = ti.xcom_pull(task_ids="load_csv",        key="row_count_raw")
    cleaned      = ti.xcom_pull(task_ids="clean_data",     key="row_count_cleaned")
    cleaned_path = ti.xcom_pull(task_ids="clean_data",     key="cleaned_output_path")
    report_path  = ti.xcom_pull(task_ids="generate_report", key="report_path")
    dropped = (raw or 0) - (cleaned or 0)

    print("=" * 50)
    print("          RÉSUMÉ DU PIPELINE NETFLIX")
    print("=" * 50)
    print(f"  Lignes chargées   : {raw}")
    print(f"  Lignes nettoyées  : {cleaned}")
    print(f"  Lignes supprimées : {dropped}")
    print(f"  Base de données   : {DB_PATH}")
    print(f"  CSV nettoyé       : {cleaned_path}")
    print(f"  Rapport           : {report_path}")
    print("=" * 50)


def task_notify_slack(**context) -> None:
    """Envoie les métriques du pipeline sur Slack via Incoming Webhook."""
    import requests

    if not SLACK_WEBHOOK_URL:
        print("[notify_slack] SLACK_WEBHOOK_URL non défini — notification ignorée.")
        return

    ti = context["ti"]
    ds           = context.get("ds", "N/A")
    raw          = ti.xcom_pull(task_ids="load_csv",        key="row_count_raw")
    cleaned      = ti.xcom_pull(task_ids="clean_data",      key="row_count_cleaned")
    report_path  = ti.xcom_pull(task_ids="generate_report", key="report_path")
    dropped      = (raw or 0) - (cleaned or 0)

    failed_tasks = [
        t.task_id
        for t in context["dag_run"].get_task_instances()
        if t.state == "failed" and t.task_id not in ("summary", "notify_slack")
    ]
    status_icon = ":x: Échec" if failed_tasks else ":white_check_mark: Succès"

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"Pipeline Netflix — {ds}"},
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Statut*\n{status_icon}"},
                {"type": "mrkdwn", "text": f"*Date de run*\n{ds}"},
                {"type": "mrkdwn", "text": f"*Lignes chargées*\n{raw or 'N/A'}"},
                {"type": "mrkdwn", "text": f"*Lignes nettoyées*\n{cleaned or 'N/A'}"},
                {"type": "mrkdwn", "text": f"*Lignes supprimées*\n{dropped}"},
                {"type": "mrkdwn", "text": f"*Rapport*\n`{report_path or 'N/A'}`"},
            ],
        },
    ]

    if failed_tasks:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Tâches en échec* : {', '.join(f'`{t}`' for t in failed_tasks)}",
            },
        })

    resp = requests.post(SLACK_WEBHOOK_URL, json={"blocks": blocks}, timeout=10)
    resp.raise_for_status()
    print(f"[notify_slack] Notification envoyée (HTTP {resp.status_code})")


# ── Définition du DAG ─────────────────────────────────────────────────────────
with DAG(
    dag_id="netflix_pipeline",
    description="Pipeline ETL Netflix : CSV → Nettoyage → SQLite → Rapport",
    default_args=DEFAULT_ARGS,
    schedule="@daily",          # Exécution quotidienne (modifiable)
    start_date=datetime(2025, 1, 1),
    catchup=False,              # Ne pas rejouer les runs passés
    tags=["netflix", "etl", "sqlite"],
) as dag:

    load_csv_task = PythonOperator(
        task_id="load_csv",
        python_callable=task_load_csv,
        retries=2,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=5),
    )

    clean_data_task = PythonOperator(
        task_id="clean_data",
        python_callable=task_clean_data,
        retries=1,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=10),
    )

    insert_to_db_task = PythonOperator(
        task_id="insert_to_db",
        python_callable=task_insert_to_db,
        retries=3,
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=True,
        execution_timeout=timedelta(minutes=10),
    )

    generate_report_task = PythonOperator(
        task_id="generate_report",
        python_callable=task_generate_report,
        retries=2,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=5),
    )

    summary_task = PythonOperator(
        task_id="summary",
        python_callable=task_summary,
        retries=0,
        execution_timeout=timedelta(minutes=2),
        trigger_rule=TriggerRule.ALL_DONE,
    )

    notify_slack_task = PythonOperator(
        task_id="notify_slack",
        python_callable=task_notify_slack,
        retries=1,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=2),
        trigger_rule=TriggerRule.ALL_DONE,
    )

    copy_to_dbt_seed_task = PythonOperator(
        task_id="copy_to_dbt_seed",
        python_callable=task_copy_to_dbt_seed,
        retries=2,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=3),
    )

    dbt_seed_task = BashOperator(
        task_id="dbt_seed",
        bash_command=f"{DBT_BIN} seed --profiles-dir {DBT_DIR} --project-dir {DBT_DIR}",
        retries=2,
        retry_delay=timedelta(minutes=3),
        execution_timeout=timedelta(minutes=15),
    )

    dbt_snapshot_task = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"{DBT_BIN} snapshot --profiles-dir {DBT_DIR} --project-dir {DBT_DIR}",
        retries=2,
        retry_delay=timedelta(minutes=3),
        execution_timeout=timedelta(minutes=15),
    )

    dbt_run_task = BashOperator(
        task_id="dbt_run",
        bash_command=f"{DBT_BIN} run --profiles-dir {DBT_DIR} --project-dir {DBT_DIR}",
        retries=2,
        retry_delay=timedelta(minutes=3),
        execution_timeout=timedelta(minutes=15),
    )

    # ── Dépendances explicites ────────────────────────────────────────────────
    load_csv_task.set_downstream(clean_data_task)

    # clean_data démarre deux branches en parallèle
    clean_data_task.set_downstream(insert_to_db_task)       # branche SQLite
    clean_data_task.set_downstream(copy_to_dbt_seed_task)   # branche dbt

    # Branche SQLite
    insert_to_db_task.set_downstream(generate_report_task)
    generate_report_task.set_downstream(summary_task)

    # Branche dbt / DuckDB
    copy_to_dbt_seed_task.set_downstream(dbt_seed_task)
    dbt_seed_task.set_downstream(dbt_snapshot_task)
    dbt_snapshot_task.set_downstream(dbt_run_task)
    dbt_run_task.set_downstream(summary_task)               # convergence vers summary

    summary_task.set_downstream(notify_slack_task)

