FROM apache/airflow:slim-3.2.2

USER root
RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends jq libssl3 openssl unixodbc unixodbc-dev \
    && /usr/python/bin/pip install --no-cache-dir --upgrade "pip>=26.1" \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && rm -f /usr/bin/docker

USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /requirements.txt \
    && pip install --no-cache-dir --upgrade \
        "aiohttp>=3.14.0" \
        "tornado>=6.5.6" \
        "setuptools>=78.1.0" \
        "cryptography>=48.0.1" \
        "python-multipart>=0.0.31" \
        "starlette>=1.3.1" \
        "certifi>=2025.4.26" \
        "jinja2>=3.1.6" \
        "werkzeug>=3.0.6" \
        "urllib3>=2.3.0"