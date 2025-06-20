import pandas as pd
import json

def generate_report(df: pd.DataFrame, output_path: str):
    report = {
        'nb_titres': len(df),
        'unique_countries': df['country'].nunique(),
        'titles_by_year': df['release_year'].value_counts().to_dict(),
        
    }

    with open(output_path, 'w') as f:
        json.dump(report, f, indent=2)




