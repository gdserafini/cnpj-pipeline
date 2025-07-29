FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y \
    build-essential \
    python3-dev \
    && apt-get clean

RUN pip install --upgrade pip setuptools wheel

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY src ./src
COPY config ./config

ENV PYTHONPATH=/app