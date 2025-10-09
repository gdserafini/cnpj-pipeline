from airflow import DAG
from datetime import datetime, date
from airflow.sdk import task
import subprocess
import requests
from app_config.settings import Settings


def _create_ingestion_dag(
    dag_id: str,
    schedule: str,
    start_date: datetime | None,
    catchup: bool,
    year: int,
    month: int
) -> DAG:
    start_date = start_date if start_date else datetime.now()
    
    with DAG(
        dag_id=dag_id,
        schedule=schedule,
        start_date=start_date,
        catchup=catchup,
        tags=["cnpj", "ingestion"]  
    ) as dag:
        @task(task_id=f'check_zip_{dag_id}')
        def check_zip():
            try:
                url = f'{Settings().BASE_URL}/{year}-{month:.2d}'
                response = requests.head(url)
                if response.status_code != 200:
                    raise FileNotFoundError('CNPJ zip file not avaliable')
            except requests.RequestException as e:
                raise e

        @task(task_id=f'ingestion_process_{dag_id}')
        def ingest_cnpj():
            result = subprocess.run(
                [
                    'python', '-m', 'src.ingestion.ingest_cnpj',
                    '-y', str(year), '-m', str(month)
                ], 
                capture_output=True, 
                text=True
            )
            if result.returncode != 0:
                raise RuntimeError(f'Process error - {result.stderr}')

        check_zip() >> ingest_cnpj() 
    
        return dag


today_date = date.today()
year = today_date.year
month = today_date.month

ingestion_dag_once = _create_ingestion_dag(
    dag_id='once',
    schedule='@once',
    start_date=None,
    catchup=True,
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
    month=next_month
)
