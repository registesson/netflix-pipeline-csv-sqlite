from pathlib import Path

import pandas as pd


def load_csv(path: str) -> pd.DataFrame:
    csv_path = Path(path)
    if not csv_path.exists():
        raise FileNotFoundError(f"Fichier CSV introuvable: {csv_path}")

    try:
        return pd.read_csv(str(csv_path))
    except Exception as exc:
        raise RuntimeError(f"Impossible de lire le CSV: {csv_path}") from exc



