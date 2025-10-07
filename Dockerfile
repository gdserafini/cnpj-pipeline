FROM apache/airflow:3.0.6-python3.11

USER root

RUN apt-get update && \
    apt-get install -y build-essential python3-dev && \
    apt-get clean

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY src /opt/airflow/src
COPY app_config /opt/airflow/app_config
COPY .env /opt/airflow/.env

ENV PYTHONPATH=/opt/airflow