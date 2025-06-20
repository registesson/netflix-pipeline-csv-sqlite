import sqlite3
import pandas as pd
import json

def insert_to_db(df: pd.DataFrame, db_path: str):
    conn = sqlite3.connect(db_path)
    df.to_sql('netflix_titles', conn, if_exists='replace', index=False)
    conn.close()

def read_from_db(db_path: str, query: str = None) -> pd.DataFrame:
    """
    Read data from the SQLite database.
    
    Args:
        db_path (str): Path to the SQLite database file
        query (str, optional): SQL query to execute. If None, reads all data from netflix_titles table
    
    Returns:
        pd.DataFrame: DataFrame containing the query results
    """
    conn = sqlite3.connect(db_path)
    
    if query is None:
        # If no query provided, read all data from netflix_titles table
        df = pd.read_sql_query("SELECT * FROM netflix_titles", conn)
    else:
        # Execute the provided query
        df = pd.read_sql_query(query, conn)
    
    conn.close()
    return df

