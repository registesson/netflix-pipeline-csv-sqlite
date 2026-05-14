FROM apache/airflow:3.0.0

# Install pipeline dependencies (airflow already provided by base image)
RUN pip install --no-cache-dir \
    "pandas>=2.0.0" \
    "pandera>=0.18.0" \
    "python-dotenv>=1.0.0" \
    "jinja2>=3.1.0" \
    "duckdb>=1.5.2"
