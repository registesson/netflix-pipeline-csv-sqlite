import sys
from pathlib import Path

# Expose dags/ so tests can import netflix_pipeline_dag without a package __init__
sys.path.insert(0, str(Path(__file__).resolve().parent / "dags"))