from pipeline.loader import load_csv
from pipeline.clean import clean_data
from pipeline.db import insert_to_db, read_from_db
from pipeline.report import generate_report
import pandas as pd

INPUT_PATH = 'data/netflix_titles.csv'
OUTPUT_PATH = 'outputs/cleaned_data.csv'
DB_PATH = 'data/netflix.db'
REPORT_PATH = 'outputs/report.json'

def run_pipeline():
   
    
    # Chargement des données
    df = load_csv(INPUT_PATH)

    # Nettoyage des données
    df_cleaned = clean_data(df)
    df_cleaned.to_csv(OUTPUT_PATH, index=False)
    # Insertion des données dans la base de données
    insert_to_db(df_cleaned, DB_PATH)

    # Génération du rapport
    generate_report(df_cleaned, REPORT_PATH)

    # Read all data from the database
    df = read_from_db('data/netflix.db')
  

    # Example 1: Read only movies from 2020
    df = read_from_db('data/netflix.db', 
        "SELECT * FROM netflix_titles WHERE release_year = 2020")

    # Example 2: Read only titles and their types
    df = read_from_db('data/netflix.db', 
        "SELECT title, type FROM netflix_titles")

    # Example 3: Read movies with specific conditions
    df = read_from_db('data/netflix.db', 
        "SELECT * FROM netflix_titles WHERE type = 'Movie' AND release_year >= 2020")
    
    df = read_from_db('data/netflix.db', 
        "SELECT count(*) FROM netflix_titles WHERE country = 'Japan'")

    print(df.head())
    

if __name__ == '__main__':
    run_pipeline()


