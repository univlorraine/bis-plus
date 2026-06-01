FROM apache/airflow:3.1.7

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends jq \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    requests \
    oauthlib \
    requests-oauthlib \
    oracledb \
    pyodbc
