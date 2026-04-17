import json
from pathlib import Path

import pandas as pd


def generate_report(df: pd.DataFrame, output_path: str) -> None:
    repartition_par_type = {
        str(key): int(value)
        for key, value in df["type"].value_counts(dropna=False).to_dict().items()
    }
    top_10_pays = {
        str(key): int(value)
        for key, value in df["country"].value_counts(dropna=True).head(10).to_dict().items()
    }
    titres_par_annee = {
        str(key): int(value)
        for key, value in df["release_year"].value_counts(dropna=True).sort_index().to_dict().items()
    }

    report = {
        "nb_titres": int(len(df)),
        "nb_pays_uniques": int(df["country"].nunique(dropna=True)),
        "repartition_par_type": repartition_par_type,
        "top_10_pays": top_10_pays,
        "titres_par_annee": titres_par_annee,
        "taux_date_added_manquante": float(df["date_added"].isna().mean()),
    }

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    with output.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)




