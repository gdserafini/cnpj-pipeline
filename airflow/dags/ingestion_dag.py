from airflow import DAG
from datetime import datetime
from airflow.sdk import task
import subprocess


def _run_process(command: list[str]) -> None:
    result = subprocess.run(
        command, capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f'Process error - {result.stderr}')


with DAG(
    dag_id='cnpj_ingestion_dag',
    schedule_interval='@monthly',
    start_date=datetime(2025, 10, 1),
    catchup=False
): 
    @task(task_id='ingestion')
    def ingest_cnpj():
        _run_process(
            [
                'python', '-m', 'src.ingestion.ingest_cnpj'
            ]
        )

    ingest_cnpj()
