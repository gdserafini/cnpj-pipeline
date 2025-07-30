FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y \
    build-essential \
    python3-dev \
    openjdk-17-jdk \
    && apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN pip install --upgrade pip setuptools wheel

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY src ./src
COPY config ./config

ENV PYTHONPATH=/app