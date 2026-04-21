import pandas as pd
import pytest
from pathlib import Path
from pipeline.loader import load_csv


def test_load_csv_returns_dataframe(tmp_path):
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("title,type\nInception,Movie\n")
    df = load_csv(str(csv_file))
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["title", "type"]
    assert len(df) == 1


def test_load_csv_file_not_found():
    with pytest.raises(FileNotFoundError):
        load_csv("data/nonexistent.csv")


def test_load_csv_invalid_file(tmp_path):
    bad_file = tmp_path / "bad.csv"
    bad_file.write_bytes(b"\x00\x01\x02")  # binaire invalide comme CSV
    # pandas peut lire certains binaires — on vérifie juste que ça ne crash pas silencieusement
    try:
        df = load_csv(str(bad_file))
        assert isinstance(df, pd.DataFrame)
    except RuntimeError:
        pass

