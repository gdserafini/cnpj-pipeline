from python:3.12-slim

WORKDIR /app

RUN addgroup --gid 1000 appgroup && \
    adduser --uid 1000 --gid 1000 --disabled-password --gecos "" appuser

RUN pip install requests

COPY ingest_cnpj.py .

USER appuser

CMD ["python", "-u", "ingest_cnpj.py"]