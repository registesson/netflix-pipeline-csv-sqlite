import sqlite3
import pandas as pd

TABLE_NAME = "netflix_titles"


def insert_to_db(df: pd.DataFrame, db_path: str, if_exists: str = "replace") -> None:
    if if_exists not in {"replace", "append", "fail"}:
        raise ValueError("if_exists doit etre 'replace', 'append' ou 'fail'.")

    with sqlite3.connect(db_path) as conn:
        df.to_sql(TABLE_NAME, conn, if_exists=if_exists, index=False)


def read_from_db(db_path: str, query: str | None = None) -> pd.DataFrame:
    """
    Read data from the SQLite database.
    
    Args:
        db_path (str): Path to the SQLite database file
        query (str, optional): SQL query to execute. If None, reads all data from netflix_titles table
    
    Returns:
        pd.DataFrame: DataFrame containing the query results
    """
    sql = query or f"SELECT * FROM {TABLE_NAME}"
    with sqlite3.connect(db_path) as conn:
        return pd.read_sql_query(sql, conn)

