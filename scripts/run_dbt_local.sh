#!/usr/bin/env zsh
set -euo pipefail

cd "$(dirname "$0")/.."

python main.py --cleaned-output dbt/seeds/netflix_titles.csv

cd dbt
export DBT_PROFILES_DIR="$(pwd)"
dbt debug
dbt seed --full-refresh
dbt run
dbt test

