import pandas as pd

REQUIRED_COLUMNS = {"title", "date_added", "release_year", "country", "type"}

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
    return cleaned
   


