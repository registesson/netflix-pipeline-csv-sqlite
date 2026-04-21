import json
import pandas as pd
import pytest
from pathlib import Path
from pipeline.report import generate_report


def make_df():
    return pd.DataFrame({
        "title": ["Inception", "Interstellar", "Stranger Things"],
        "type": ["Movie", "Movie", "TV Show"],
        "country": ["USA", "USA", "USA"],
        "release_year": [2010, 2014, 2016],
        "date_added": pd.to_datetime(["2021-01-01", "2021-02-05", None]),
    })


def test_generate_report_creates_json(tmp_path):
    df = make_df()
    output = tmp_path / "report.json"
    generate_report(df, str(output))
    assert output.exists()


def test_generate_report_creates_html(tmp_path):
    df = make_df()
    output = tmp_path / "report.json"
    generate_report(df, str(output))
    html_output = tmp_path / "report.html"
    assert html_output.exists()
    content = html_output.read_text()
    assert "Netflix" in content


def test_report_json_content(tmp_path):
    df = make_df()
    output = tmp_path / "report.json"
    generate_report(df, str(output))
    with output.open() as f:
        report = json.load(f)
    assert report["nb_titres"] == 3
    assert report["nb_pays_uniques"] == 1
    assert "Movie" in report["repartition_par_type"]
    assert "TV Show" in report["repartition_par_type"]
    assert report["taux_date_added_manquante"] == pytest.approx(1 / 3, rel=1e-3)

