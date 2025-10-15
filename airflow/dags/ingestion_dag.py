from airflow import DAG
from datetime import datetime
from airflow.sdk import task
import subprocess
import requests
from app_config.settings import Settings
import calendar


def _create_ingestion_dag(
    dag_id: str,
    schedule: str,
    start_date: datetime,
    catchup: bool,
    year: int,
    month: int
) -> DAG:
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


now = datetime.now()
year = now.year
month = now.month
day = now.day
hour = now.hour
minute = now.minute

ingestion_dag_once = _create_ingestion_dag(
    dag_id='once',
    schedule='@once',
    start_date=datetime(year, month, day, hour, minute+5),
    catchup=True,
    year=year,
    month=month   
)

next_month = 1 if month + 1 == 13 else month + 1
year = year + 1 if next_month == 1 else year
_, last_day = calendar.monthrange(year, month)

ingestion_dag_monthly = _create_ingestion_dag(
    dag_id='monthly',
    schedule='@monthly',
    start_date=datetime(year, next_month, last_day),
    catchup=False,
    year=year,
    month=next_month
)
