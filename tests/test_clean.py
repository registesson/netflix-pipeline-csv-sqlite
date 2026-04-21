import pandas as pd
import pytest
from pipeline.clean import clean_data


def make_df(**kwargs):
    base = {
        "title": ["Inception", "Interstellar", "Tenet"],
        "type": ["Movie", "Movie", "Movie"],
        "country": ["USA", "USA", None],
        "release_year": [2010, 2014, 2020],
        "date_added": ["January 1, 2021", "February 5, 2021", "March 10, 2021"],
    }
    base.update(kwargs)
    return pd.DataFrame(base)


def test_clean_data_basic():
    df = make_df()
    result = clean_data(df)
    assert len(result) == 3
    assert result["country"].isna().sum() == 0


def test_clean_data_fills_unknown_country():
    df = make_df()
    result = clean_data(df)
    assert "Unknown" in result["country"].values


def test_clean_data_drops_null_title():
    df = make_df(title=["Inception", None, "Tenet"])
    result = clean_data(df)
    assert len(result) == 2
    assert result["title"].isna().sum() == 0


def test_clean_data_parses_date_added():
    df = make_df()
    result = clean_data(df)
    assert pd.api.types.is_datetime64_any_dtype(result["date_added"])


def test_clean_data_drops_duplicates():
    df = make_df(
        title=["Inception", "Inception", "Tenet"],
        date_added=["January 1, 2021", "January 1, 2021", "March 10, 2021"],
    )
    result = clean_data(df)
    assert len(result) == 2


def test_clean_data_missing_column_raises():
    df = pd.DataFrame({"title": ["Inception"], "type": ["Movie"]})
    with pytest.raises(ValueError, match="Colonnes manquantes"):
        clean_data(df)


def test_clean_data_release_year_as_int():
    df = make_df()
    result = clean_data(df)
    assert result["release_year"].dtype.name == "Int64"

