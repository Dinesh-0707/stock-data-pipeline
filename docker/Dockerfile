FROM apache/airflow:2.8.0

USER root

RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt
COPY requirements-dbt.txt /requirements-dbt.txt

RUN pip install --no-cache-dir -r /requirements.txt

# dbt gets its own virtualenv so its dependencies can't clash with Airflow's
RUN python -m venv /opt/airflow/dbt_venv \
    && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir -r /requirements-dbt.txt
