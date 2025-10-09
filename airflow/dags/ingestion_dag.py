from airflow import DAG
from datetime import datetime, date
from airflow.sdk import task
import subprocess
import requests


def _run_process(command: list[str]) -> None:
    result = subprocess.run(
        command, 
        capture_output=True, 
        text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f'Process error - {result.stderr}')


def _check_zip(year: int, month: int) -> bool:
    ...


def _create_ingestion_dag(
    dag_id: str,
    schedule: str,
    start_date: datetime | None,
    catchup: bool,
    year: int,
    month: int
) -> DAG:
    with DAG(
        dag_id=dag_id,
        schedule=schedule,
        start_date=start_date if start_date else datetime.now(),
        catchup=catchup,
        tags=["cnpj", "ingestion"]  
    ) as dag:
        @task(task_id=f'check_zip_{dag_id}')
        def check_zip():
            if not _check_zip(year, month):
                raise Exception('CNPJ zip file not avaliable')

        @task(task_id=f'ingestion_process_{dag_id}')
        def ingest_cnpj():
            _run_process(
                [
                    'python', '-m', 'src.ingestion.ingest_cnpj',
                    '-y', str(year), '-m', str(month)
                ]
            )

        check_zip() >> ingest_cnpj() 
    
        return dag


today_date = date.today()
year = today_date.year
month = today_date.month

ingestion_dag_once = _create_ingestion_dag(
    dag_id='once',
    schedule='@once',
    start_date=None,
    catchup=False,
    year=year,
    month=month   
)

next_month = 1 if month + 1 == 13 else month + 1
year = year + 1 if next_month == 1 else year

ingestion_dag_monthly = _create_ingestion_dag(
    dag_id='monthly',
    schedule='@monthly',
    start_date=datetime(year, next_month, 1),
    catchup=False,
    year=year,
    month=month   
)
