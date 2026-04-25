"""
DAG Airflow - Pipeline Netflix
CSV → Nettoyage → SQLite → Rapport JSON/HTML

Structure des tâches :
    load_csv >> clean_data >> insert_to_db >> generate_report
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

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

# ── Arguments par défaut du DAG ───────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── Définition des callables (une fonction = une tâche) ───────────────────────

def task_load_csv(**context) -> None:
    """Charge le fichier CSV et vérifie qu'il est lisible."""
    from pipeline.loader import load_csv

    df = load_csv(INPUT_PATH)
    context["ti"].xcom_push(key="row_count_raw", value=len(df))
    print(f"[load_csv] {len(df)} lignes chargées depuis {INPUT_PATH}")


def task_clean_data(**context) -> None:
    """Nettoie les données et les sauvegarde en CSV intermédiaire."""
    from pipeline.loader import load_csv
    from pipeline.clean import clean_data

    Path(CLEANED_OUTPUT).parent.mkdir(parents=True, exist_ok=True)

    df_raw = load_csv(INPUT_PATH)
    df_cleaned = clean_data(df_raw)
    df_cleaned.to_csv(CLEANED_OUTPUT, index=False)

    context["ti"].xcom_push(key="row_count_cleaned", value=len(df_cleaned))
    print(f"[clean_data] {len(df_cleaned)} lignes nettoyées → {CLEANED_OUTPUT}")


def task_insert_to_db(**context) -> None:
    """Insère les données nettoyées dans la base SQLite."""
    import pandas as pd
    from pipeline.db import insert_to_db

    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

    df_cleaned = pd.read_csv(CLEANED_OUTPUT)
    insert_to_db(df_cleaned, DB_PATH, IF_EXISTS)

    print(f"[insert_to_db] {len(df_cleaned)} lignes insérées dans {DB_PATH} (mode={IF_EXISTS})")


def task_generate_report(**context) -> None:
    """Génère le rapport JSON et HTML depuis les données nettoyées."""
    import pandas as pd
    from pipeline.report import generate_report

    Path(REPORT_PATH).parent.mkdir(parents=True, exist_ok=True)

    df_cleaned = pd.read_csv(CLEANED_OUTPUT)
    generate_report(df_cleaned, REPORT_PATH)

    print(f"[generate_report] Rapport généré → {REPORT_PATH}")


def task_summary(**context) -> None:
    """Affiche un résumé des métriques du pipeline via XCom."""
    ti = context["ti"]
    raw     = ti.xcom_pull(task_ids="load_csv",    key="row_count_raw")
    cleaned = ti.xcom_pull(task_ids="clean_data",  key="row_count_cleaned")
    dropped = (raw or 0) - (cleaned or 0)

    print("=" * 50)
    print("          RÉSUMÉ DU PIPELINE NETFLIX")
    print("=" * 50)
    print(f"  Lignes chargées   : {raw}")
    print(f"  Lignes nettoyées  : {cleaned}")
    print(f"  Lignes supprimées : {dropped}")
    print(f"  Base de données   : {DB_PATH}")
    print(f"  Rapport           : {REPORT_PATH}")
    print("=" * 50)


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
    )

    clean_data_task = PythonOperator(
        task_id="clean_data",
        python_callable=task_clean_data,
    )

    insert_to_db_task = PythonOperator(
        task_id="insert_to_db",
        python_callable=task_insert_to_db,
    )

    generate_report_task = PythonOperator(
        task_id="generate_report",
        python_callable=task_generate_report,
    )

    summary_task = PythonOperator(
        task_id="summary",
        python_callable=task_summary,
    )

    # ── Ordre d'exécution ─────────────────────────────────────────────────────
    load_csv_task >> clean_data_task >> insert_to_db_task >> generate_report_task >> summary_task

