from airflow import DAG
from datetime import datetime
from airflow.sdk import task
import subprocess
import requests
from app_config.settings import Settings
from src.utils.logging import logger


def _create_ingestion_dag(
    dag_id: str,
    schedule: str,
    start_date: datetime,
    catchup: bool,
    year: int,
    month: int
) -> DAG:
    from pendulum import timezone
    local_tz = timezone('America/Sao_Paulo')

    start_date = start_date.replace(tzinfo=local_tz)

    with DAG(
        dag_id=dag_id,
        schedule=schedule,
        start_date=start_date,
        catchup=catchup,
        is_paused_upon_creation=False,
        tags=["cnpj", "ingestion"]  
    ) as dag:
        @task(task_id=f'check_zip_{dag_id}')
        def check_zip():
            try:
                url = f'{Settings().BASE_URL}/{year}-{month:02d}'
                logger.info(f'Requesting url {url}')
                response = requests.head(url)
                status_code = response.status_code
                logger.info(f'Response status code {status_code}')
                if not status_code in [200, 301]:
                    raise FileNotFoundError('CNPJ zip file not avaliable')
            except requests.RequestException as e:
                logger.error(f'{e}')
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
            returncode = result.returncode
            logger.info(f'Process returncode {returncode}')
            if returncode != 0:
                error = result.stderr
                logger.error(f'{error}')
                raise RuntimeError(f'Process error - {error}')

        check_zip() >> ingest_cnpj() 
    
        return dag


now = datetime.now()

cyear = now.year
cmonth = now.month

prev_month = cmonth - 1 if cmonth - 1 > 0 else 12
prev_year = cyear - 1 if prev_month == 12 else cyear

next_month = cmonth + 1 if cmonth + 1 < 12 else 1
next_year = cyear + 1 if next_month == 1 else cyear


ingestion_dag_once = _create_ingestion_dag(
    dag_id='once',
    schedule='@once',
    start_date=datetime(1900, 1, 1),
    catchup=True,
    year=prev_year,
    month=prev_month   
) 

ingestion_dag_monthly = _create_ingestion_dag(
    dag_id='monthly',
    schedule='@monthly',
    start_date=datetime(next_year, next_month, 1),
    catchup=False,
    year=cyear,
    month=cmonth
)
