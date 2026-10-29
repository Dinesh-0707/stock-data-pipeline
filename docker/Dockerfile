# Python 3.11 image: the default 2.8.0 tag is Python 3.8, which current
# duckdb/deltalake no longer ship wheels for
FROM apache/airflow:2.8.0-python3.11

COPY requirements.txt /requirements.txt
COPY requirements-dbt.txt /requirements-dbt.txt

# Pipeline scripts and dbt run in their own virtualenv, so their packages
# can't upgrade or clash with the ones Airflow itself depends on.
# PIP_USER=false because the Airflow image defaults to --user installs.
RUN python -m venv /opt/airflow/venv \
    && PIP_USER=false /opt/airflow/venv/bin/pip install --no-cache-dir \
        -r /requirements.txt -r /requirements-dbt.txt
