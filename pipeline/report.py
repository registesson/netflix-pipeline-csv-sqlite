import json
from datetime import datetime
from pathlib import Path

import pandas as pd
from jinja2 import Environment, FileSystemLoader


TEMPLATE_DIR = Path(__file__).parent / "templates"


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

    # JSON
    with output.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    # HTML
    html_output = output.with_suffix(".html")
    env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)))
    template = env.get_template("report.html.j2")
    html_content = template.render(
        **report,
        generated_at=datetime.now().strftime("%d/%m/%Y %H:%M"),
    )
    with html_output.open("w", encoding="utf-8") as f:
        f.write(html_content)
