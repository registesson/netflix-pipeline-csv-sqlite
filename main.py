import argparse
import logging
import os
from pathlib import Path

from dotenv import load_dotenv

from pipeline.clean import clean_data
from pipeline.db import insert_to_db, read_from_db
from pipeline.loader import load_csv
from pipeline.report import generate_report

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="CSV -> nettoyage -> SQLite -> rapport JSON"
    )
    parser.add_argument("--input", default=os.getenv("INPUT_PATH", "data/netflix_titles.csv"))
    parser.add_argument("--cleaned-output", default=os.getenv("CLEANED_OUTPUT", "outputs/cleaned_data.csv"))
    parser.add_argument("--db", default=os.getenv("DB_PATH", "data/netflix.db"))
    parser.add_argument("--report", default=os.getenv("REPORT_PATH", "outputs/report.json"))
    parser.add_argument("--if-exists", default=os.getenv("IF_EXISTS", "replace"), choices=["replace", "append", "fail"])
    parser.add_argument("--show-examples", action="store_true")
    return parser.parse_args()


def run_pipeline(args: argparse.Namespace) -> None:
    Path(args.cleaned_output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.report).parent.mkdir(parents=True, exist_ok=True)
    Path(args.db).parent.mkdir(parents=True, exist_ok=True)

    logger.info("Chargement CSV...")
    df_raw = load_csv(args.input)

    logger.info("Nettoyage des donnees...")
    df_cleaned = clean_data(df_raw)
    df_cleaned.to_csv(args.cleaned_output, index=False)

    logger.info("Insertion SQLite (%s)...", args.if_exists)
    insert_to_db(df_cleaned, args.db, args.if_exists)

    logger.info("Generation du rapport JSON...")
    generate_report(df_cleaned, args.report)

    logger.info("Top 10 pays par nombre de titres:")
    df_all = read_from_db(args.db)
    country_counts = df_all.groupby("country", dropna=False).agg(title_count=("title", "count"))
    print(country_counts.sort_values("title_count", ascending=False).head(10))

    if args.show_examples:
        logger.info("Exemples de requetes SQL...")
        df_movies_2020 = read_from_db(
            args.db,
            "SELECT * FROM netflix_titles WHERE release_year = 2020",
        )
        logger.info("Movies 2020: %d lignes", len(df_movies_2020))

        df_titles_types = read_from_db(
            args.db,
            "SELECT title, type FROM netflix_titles",
        )
        logger.info("Titres + type: %d lignes", len(df_titles_types))

        df_recent_movies = read_from_db(
            args.db,
            "SELECT * FROM netflix_titles WHERE type = 'Movie' AND release_year >= 2020",
        )
        logger.info("Movies >= 2020: %d lignes", len(df_recent_movies))

        df_japan_count = read_from_db(
            args.db,
            "SELECT COUNT(*) AS n FROM netflix_titles WHERE country = 'Japan'",
        )
        print(df_japan_count)


if __name__ == "__main__":
    run_pipeline(parse_args())
