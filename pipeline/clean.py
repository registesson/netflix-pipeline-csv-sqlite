import pandas as pd

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=['title']) # Supprimer les lignes sans titre
    df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')
    df = df.drop_duplicates(subset=['title', 'date_added'])
    return df
   


