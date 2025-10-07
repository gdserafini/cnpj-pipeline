import logging


logging.basicConfig(
    filename='/opt/airflow/app_logs/ingestion.log',
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - Log: %(message)s"
)


logger = logging.getLogger(__name__)
