"""
Tests unitaires des callables du DAG Airflow.

Les fonctions task_* sont appelées directement avec un contexte Airflow simulé
(MagicMock pour ti) — aucune infrastructure Airflow réelle n'est requise.
Les variables de chemin du module (INPUT_PATH, DB_PATH, …) sont surchargées
via monkeypatch ; les fonctions pipeline sont mockées au besoin.
"""

import pandas as pd
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

import netflix_pipeline_dag as dag_mod


# ── Helpers ──────────────────────────────────────────────────────────────────

def make_context(xcom_values=None):
    """Contexte Airflow minimal avec un TaskInstance mocké."""
    ti = MagicMock()
    if xcom_values:
        ti.xcom_pull.side_effect = lambda task_ids, key: xcom_values.get(key)
    return {"ti": ti, "ds": "2024-01-01"}


def xcom_pushes(ti):
    """Retourne les appels xcom_push sous forme {key: value}."""
    return {c.kwargs["key"]: c.kwargs["value"] for c in ti.xcom_push.call_args_list}


# ── on_task_failure ──────────────────────────────────────────────────────────

def test_on_task_failure_logs_key_info(capsys):
    ti = MagicMock()
    ti.dag_id = "netflix_pipeline"
    ti.task_id = "load_csv"
    ti.run_id = "manual__2024-01-01"
    ti.try_number = 2
    ti.max_tries = 2
    dag_mod.on_task_failure({"task_instance": ti, "exception": ValueError("boom")})
    out = capsys.readouterr().out
    assert "netflix_pipeline" in out
    assert "load_csv" in out
    assert "boom" in out


# ── task_load_csv ────────────────────────────────────────────────────────────

def test_load_csv_raises_when_file_missing(monkeypatch):
    monkeypatch.setattr(dag_mod, "INPUT_PATH", "/nonexistent/path.csv")
    with pytest.raises(FileNotFoundError, match="Fichier source introuvable"):
        dag_mod.task_load_csv(**make_context())


def test_load_csv_raises_when_csv_empty(tmp_path, monkeypatch):
    csv_file = tmp_path / "empty.csv"
    csv_file.touch()
    monkeypatch.setattr(dag_mod, "INPUT_PATH", str(csv_file))
    with patch("pipeline.loader.load_csv", return_value=pd.DataFrame()):
        with pytest.raises(ValueError, match="vide"):
            dag_mod.task_load_csv(**make_context())


def test_load_csv_raises_on_read_error(tmp_path, monkeypatch):
    csv_file = tmp_path / "bad.csv"
    csv_file.touch()
    monkeypatch.setattr(dag_mod, "INPUT_PATH", str(csv_file))
    with patch("pipeline.loader.load_csv", side_effect=Exception("encoding error")):
        with pytest.raises(RuntimeError, match="Impossible de lire"):
            dag_mod.task_load_csv(**make_context())


def test_load_csv_pushes_row_count(tmp_path, monkeypatch):
    csv_file = tmp_path / "titles.csv"
    csv_file.touch()
    monkeypatch.setattr(dag_mod, "INPUT_PATH", str(csv_file))
    df = pd.DataFrame({"title": ["A", "B", "C"]})
    ctx = make_context()
    with patch("pipeline.loader.load_csv", return_value=df):
        dag_mod.task_load_csv(**ctx)
    assert xcom_pushes(ctx["ti"])["row_count_raw"] == 3


# ── task_clean_data ──────────────────────────────────────────────────────────

@pytest.fixture()
def clean_data_patches(tmp_path, monkeypatch):
    """Patche INPUT_PATH et CLEANED_OUTPUT vers tmp_path."""
    csv_file = tmp_path / "titles.csv"
    csv_file.touch()
    monkeypatch.setattr(dag_mod, "INPUT_PATH", str(csv_file))
    monkeypatch.setattr(dag_mod, "CLEANED_OUTPUT", str(tmp_path / "cleaned.csv"))
    return tmp_path


def test_clean_data_raises_on_missing_columns(clean_data_patches):
    raw_df = pd.DataFrame({"title": ["A"]})
    with patch("pipeline.loader.load_csv", return_value=raw_df):
        with patch("pipeline.clean.clean_data", side_effect=ValueError("Colonnes manquantes")):
            with pytest.raises(ValueError, match="colonnes manquantes ou invalides"):
                dag_mod.task_clean_data(**make_context())


def test_clean_data_raises_on_unexpected_error(clean_data_patches):
    raw_df = pd.DataFrame({"title": ["A"]})
    with patch("pipeline.loader.load_csv", return_value=raw_df):
        with patch("pipeline.clean.clean_data", side_effect=RuntimeError("OOM")):
            with pytest.raises(RuntimeError, match="Erreur inattendue"):
                dag_mod.task_clean_data(**make_context())


def test_clean_data_raises_when_result_empty(clean_data_patches):
    raw_df = pd.DataFrame({"title": ["A"]})
    with patch("pipeline.loader.load_csv", return_value=raw_df):
        with patch("pipeline.clean.clean_data", return_value=pd.DataFrame()):
            with pytest.raises(ValueError, match="vide"):
                dag_mod.task_clean_data(**make_context())


def test_clean_data_pushes_xcoms(clean_data_patches, tmp_path):
    raw_df = pd.DataFrame({"title": ["A"]})
    cleaned_df = pd.DataFrame({"title": ["A"], "type": ["Movie"]})
    ctx = make_context()
    with patch("pipeline.loader.load_csv", return_value=raw_df):
        with patch("pipeline.clean.clean_data", return_value=cleaned_df):
            dag_mod.task_clean_data(**ctx)
    pushes = xcom_pushes(ctx["ti"])
    assert pushes["row_count_cleaned"] == 1
    assert "cleaned_2024-01-01.csv" in pushes["cleaned_output_path"]


# ── task_insert_to_db ────────────────────────────────────────────────────────

def test_insert_to_db_raises_when_xcom_none(tmp_path, monkeypatch):
    monkeypatch.setattr(dag_mod, "DB_PATH", str(tmp_path / "test.db"))
    ctx = make_context(xcom_values={"cleaned_output_path": None})
    with pytest.raises(FileNotFoundError, match="introuvable via XCom"):
        dag_mod.task_insert_to_db(**ctx)


def test_insert_to_db_raises_when_path_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(dag_mod, "DB_PATH", str(tmp_path / "test.db"))
    ctx = make_context(xcom_values={"cleaned_output_path": "/nonexistent/cleaned.csv"})
    with pytest.raises(FileNotFoundError, match="introuvable via XCom"):
        dag_mod.task_insert_to_db(**ctx)


def test_insert_to_db_raises_on_db_error(tmp_path, monkeypatch):
    cleaned_csv = tmp_path / "cleaned.csv"
    pd.DataFrame({"title": ["A"]}).to_csv(cleaned_csv, index=False)
    monkeypatch.setattr(dag_mod, "DB_PATH", str(tmp_path / "test.db"))
    ctx = make_context(xcom_values={"cleaned_output_path": str(cleaned_csv)})
    with patch("pipeline.db.insert_to_db", side_effect=Exception("DB locked")):
        with pytest.raises(RuntimeError, match="Échec de l'insertion"):
            dag_mod.task_insert_to_db(**ctx)


def test_insert_to_db_happy_path(tmp_path, monkeypatch):
    cleaned_csv = tmp_path / "cleaned.csv"
    pd.DataFrame({"title": ["A"]}).to_csv(cleaned_csv, index=False)
    monkeypatch.setattr(dag_mod, "DB_PATH", str(tmp_path / "test.db"))
    ctx = make_context(xcom_values={"cleaned_output_path": str(cleaned_csv)})
    with patch("pipeline.db.insert_to_db") as mock_insert:
        dag_mod.task_insert_to_db(**ctx)
    mock_insert.assert_called_once()


# ── task_generate_report ─────────────────────────────────────────────────────

def test_generate_report_raises_when_xcom_none(tmp_path, monkeypatch):
    monkeypatch.setattr(dag_mod, "REPORT_PATH", str(tmp_path / "report.json"))
    ctx = make_context(xcom_values={"cleaned_output_path": None})
    with pytest.raises(FileNotFoundError, match="introuvable via XCom"):
        dag_mod.task_generate_report(**ctx)


def test_generate_report_raises_on_error(tmp_path, monkeypatch):
    cleaned_csv = tmp_path / "cleaned.csv"
    pd.DataFrame({"title": ["A"]}).to_csv(cleaned_csv, index=False)
    monkeypatch.setattr(dag_mod, "REPORT_PATH", str(tmp_path / "report.json"))
    ctx = make_context(xcom_values={"cleaned_output_path": str(cleaned_csv)})
    with patch("pipeline.report.generate_report", side_effect=Exception("template error")):
        with pytest.raises(RuntimeError, match="Échec de la génération du rapport"):
            dag_mod.task_generate_report(**ctx)


def test_generate_report_pushes_report_path(tmp_path, monkeypatch):
    cleaned_csv = tmp_path / "cleaned.csv"
    pd.DataFrame({"title": ["A"]}).to_csv(cleaned_csv, index=False)
    monkeypatch.setattr(dag_mod, "REPORT_PATH", str(tmp_path / "report.json"))
    ctx = make_context(xcom_values={"cleaned_output_path": str(cleaned_csv)})
    with patch("pipeline.report.generate_report"):
        dag_mod.task_generate_report(**ctx)
    pushes = xcom_pushes(ctx["ti"])
    assert "report_2024-01-01.json" in pushes["report_path"]


# ── task_copy_to_dbt_seed ────────────────────────────────────────────────────

def test_copy_to_dbt_seed_raises_when_xcom_none(tmp_path, monkeypatch):
    monkeypatch.setattr(dag_mod, "DBT_SEED_PATH", str(tmp_path / "seeds" / "netflix_titles.csv"))
    ctx = make_context(xcom_values={"cleaned_output_path": None})
    with pytest.raises(FileNotFoundError, match="introuvable via XCom"):
        dag_mod.task_copy_to_dbt_seed(**ctx)


def test_copy_to_dbt_seed_raises_on_oserror(tmp_path, monkeypatch):
    cleaned_csv = tmp_path / "cleaned.csv"
    cleaned_csv.write_text("title\nA\n")
    monkeypatch.setattr(dag_mod, "DBT_SEED_PATH", str(tmp_path / "seeds" / "netflix_titles.csv"))
    ctx = make_context(xcom_values={"cleaned_output_path": str(cleaned_csv)})
    with patch("shutil.copy2", side_effect=OSError("disk full")):
        with pytest.raises(RuntimeError, match="Échec de la copie"):
            dag_mod.task_copy_to_dbt_seed(**ctx)


def test_copy_to_dbt_seed_copies_file(tmp_path, monkeypatch):
    cleaned_csv = tmp_path / "cleaned.csv"
    cleaned_csv.write_text("title\nA\n")
    seed_path = tmp_path / "seeds" / "netflix_titles.csv"
    monkeypatch.setattr(dag_mod, "DBT_SEED_PATH", str(seed_path))
    ctx = make_context(xcom_values={"cleaned_output_path": str(cleaned_csv)})
    dag_mod.task_copy_to_dbt_seed(**ctx)
    assert seed_path.exists()
    assert seed_path.read_text() == "title\nA\n"


# ── task_notify_slack ────────────────────────────────────────────────────────

def make_notify_context(xcom_values=None):
    """Contexte Airflow minimal pour task_notify_slack."""
    return make_context(xcom_values=xcom_values)


def test_notify_slack_skips_when_no_webhook(monkeypatch, capsys):
    monkeypatch.setattr(dag_mod, "SLACK_WEBHOOK_URL", "")
    dag_mod.task_notify_slack(**make_notify_context())
    assert "ignorée" in capsys.readouterr().out


def test_notify_slack_posts_to_webhook(monkeypatch):
    monkeypatch.setattr(dag_mod, "SLACK_WEBHOOK_URL", "https://hooks.slack.com/test")
    ctx = make_notify_context(xcom_values={
        "row_count_raw": 100,
        "row_count_cleaned": 95,
        "report_path": "/outputs/report_2024-01-01.json",
    })
    mock_resp = MagicMock(status_code=200)
    with patch("requests.post", return_value=mock_resp) as mock_post:
        dag_mod.task_notify_slack(**ctx)
    mock_post.assert_called_once_with(
        "https://hooks.slack.com/test",
        json=mock_post.call_args.kwargs["json"],
        timeout=10,
    )


def test_notify_slack_success_message_content(monkeypatch):
    monkeypatch.setattr(dag_mod, "SLACK_WEBHOOK_URL", "https://hooks.slack.com/test")
    ctx = make_notify_context(xcom_values={
        "row_count_raw": 100,
        "row_count_cleaned": 95,
        "report_path": "/outputs/report_2024-01-01.json",
    })
    mock_resp = MagicMock(status_code=200)
    with patch("requests.post", return_value=mock_resp) as mock_post:
        dag_mod.task_notify_slack(**ctx)

    blocks = mock_post.call_args.kwargs["json"]["blocks"]
    assert "Pipeline Netflix — 2024-01-01" in blocks[0]["text"]["text"]
    fields = [f["text"] for f in blocks[1]["fields"]]
    assert any(":white_check_mark:" in t for t in fields)
    assert any("100" in t for t in fields)
    assert any("95" in t for t in fields)


def test_notify_slack_partial_failure_shows_icon(monkeypatch):
    monkeypatch.setattr(dag_mod, "SLACK_WEBHOOK_URL", "https://hooks.slack.com/test")
    # report_path manquant → heuristique échec partiel
    ctx = make_notify_context(xcom_values={
        "row_count_raw": 100,
        "row_count_cleaned": 95,
        "report_path": None,
    })
    mock_resp = MagicMock(status_code=200)
    with patch("requests.post", return_value=mock_resp) as mock_post:
        dag_mod.task_notify_slack(**ctx)

    fields = [f["text"] for f in mock_post.call_args.kwargs["json"]["blocks"][1]["fields"]]
    assert any(":x:" in t for t in fields)


def test_notify_slack_raises_on_http_error(monkeypatch):
    monkeypatch.setattr(dag_mod, "SLACK_WEBHOOK_URL", "https://hooks.slack.com/test")
    mock_resp = MagicMock()
    mock_resp.raise_for_status.side_effect = Exception("HTTP 500")
    with patch("requests.post", return_value=mock_resp):
        with pytest.raises(Exception, match="HTTP 500"):
            dag_mod.task_notify_slack(**make_notify_context())