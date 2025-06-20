# Data Pipeline: CSV to SQLite with Reporting

## Overview
This project provides a simple data pipeline that:
- Loads Netflix titles data from a CSV file
- Cleans and preprocesses the data
- Stores the cleaned data in a SQLite database
- Generates a summary report in JSON format

## Project Structure

```
data-pipeline-csv-sqlite/
│
├── main.py                # Main entry point to run the data pipeline
├── requirements.txt       # Python dependencies (currently: pandas)
├── README.md              # Project documentation
│
├── data/
│   ├── netflix_titles.csv # Raw input data (CSV)
│   └── netflix.db         # SQLite database (output)
│
├── outputs/
│   ├── cleaned_data.csv   # Cleaned CSV data (output)
│   └── report.json        # Generated report (output)
│
└── pipeline/
    ├── loader.py          # Loads CSV data into a DataFrame
    ├── clean.py           # Cleans and preprocesses the DataFrame
    ├── db.py              # Handles SQLite DB insertions and queries
    └── report.py          # Generates a JSON report from the DataFrame
```

## Setup

1. **Clone the repository**
2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
3. **Place your input CSV** in the `data/` directory (default: `netflix_titles.csv`).

## Usage

Run the pipeline with:
```bash
python main.py
```

This will:
- Load the CSV data
- Clean and preprocess it
- Save the cleaned data to `outputs/cleaned_data.csv`
- Insert the cleaned data into `data/netflix.db` (SQLite)
- Generate a summary report at `outputs/report.json`

## Pipeline Steps

1. **Loading** (`pipeline/loader.py`)
   - Loads the CSV into a pandas DataFrame.
2. **Cleaning** (`pipeline/clean.py`)
   - Drops rows without a title
   - Parses `date_added` as datetime
   - Removes duplicates based on `title` and `date_added`
3. **Database Insertion** (`pipeline/db.py`)
   - Inserts the cleaned DataFrame into a SQLite database (`netflix_titles` table)
   - Provides functions to run custom SQL queries and return results as DataFrames
4. **Reporting** (`pipeline/report.py`)
   - Generates a JSON report with:
     - Number of titles
     - Number of unique countries
     - Titles count by release year

## Example Queries

The pipeline demonstrates how to run example queries on the SQLite database, such as:
- All movies from 2020
- Titles and their types
- Movies from 2020 onwards
- Count of titles from Japan

## Requirements
- Python 3.8+
- pandas >= 2.0.0

## Customization
- To use a different input file, change the `INPUT_PATH` in `main.py`.
- To add more cleaning steps, edit `pipeline/clean.py`.
- To extend reporting, modify `pipeline/report.py`.

## License
MIT License
