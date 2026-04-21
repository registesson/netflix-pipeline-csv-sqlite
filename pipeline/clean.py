import logging

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import Column, DataFrameSchema, Check

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = {"title", "date_added", "release_year", "country", "type"}

NETFLIX_SCHEMA = DataFrameSchema(
    {
        "title": Column(str, nullable=False),
        "type": Column(str, Check.isin(["Movie", "TV Show"]), nullable=True),
        "country": Column(str, nullable=False),
        "release_year": Column(pa.Int64, Check.in_range(1900, 2030), nullable=True),
        "date_added": Column(pa.DateTime, nullable=True),
    },
    coerce=False,
)


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    missing_columns = REQUIRED_COLUMNS - set(df.columns)
    if missing_columns:
        raise ValueError(f"Colonnes manquantes dans le CSV: {sorted(missing_columns)}")

    cleaned = df.copy()
    cleaned = cleaned.dropna(subset=["title"])
    cleaned["date_added"] = pd.to_datetime(cleaned["date_added"], errors="coerce")
    cleaned["release_year"] = pd.to_numeric(cleaned["release_year"], errors="coerce").astype("Int64")
    cleaned["country"] = cleaned["country"].fillna("Unknown")
    cleaned = cleaned.drop_duplicates(subset=["title", "date_added"])

    try:
        NETFLIX_SCHEMA.validate(cleaned, lazy=True)
        logger.info("Validation du schéma réussie.")
    except pa.errors.SchemaErrors as exc:
        logger.warning("Erreurs de validation du schéma:\n%s", exc.failure_cases)

    return cleaned
