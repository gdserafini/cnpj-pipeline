from enum import Enum
import requests as req
import zipfile
import io
import os
import time
from src.utils.logging import logger
import pandas as pd
from config.settings import Settings
from pyspark.sql import SparkSession
import pyarrow as pa
import pyarrow.parquet as pq
import gc
import uuid
from typing import Any


dft_year = Settings().DFT_YEAR
dft_month = Settings().DFT_MONTH
dft_month_str = Settings().DFT_MONTH_STR
base_dir_path = Settings().BASE_DIR_PATH
base_url = Settings().BASE_URL
dft_chunk_size = Settings().DFT_CHUNK_SIZE
dft_dwld_chunk_size = Settings().DFT_DWLD_CHUNK_SIZE


class CnpjEnum(Enum):
    CNAES = 'Cnaes'
    PAISES = 'Paises'
    SIMPLES = 'Simples'
    EMPRESAS = 'Empresas0'
    ESTABELECIMENTOS = 'Estabelecimentos0'
    MOTIVOS = 'Motivos'
    MUNICIPIOS = 'Municipios'
    NATUREZAS = 'Naturezas'
    QUALIFICACOES = 'Qualificacoes'
    SOCIOS = 'Socios0'


def build_url(data: CnpjEnum, year: int, month: int) -> str:
    url = f'{base_url}/{year}-{month:02d}/{data.value}.zip'
    return url


def fetch_data_stream(
    source_url: str,
    output_path: str
) -> None:
    create_dir(os.path.dirname(output_path))
    with req.get(source_url, stream=True) as r:
        r.raise_for_status()
        with open(output_path, 'wb') as f:
            for chunk in r.iter_content(
                chunk_size=dft_dwld_chunk_size
            ):
                f.write(chunk)


def create_dir(path: str) -> None:
    if path is None or path.strip() == '':
        raise Exception('Invalid path')
    os.makedirs(path, exist_ok=True)


def get_partition_path(output_path: str) -> str:
    partition_path = os.path.join(
        output_path.rstrip('/'),
        f'year={dft_year}',
        f'month={dft_month:02d}'
    )
    return partition_path


def create_partition(output_path: str) -> None:
    os.makedirs(output_path.rstrip('/'), exist_ok=True)
    partition_path = get_partition_path(output_path)
    os.makedirs(partition_path, exist_ok=True)


def data_already_exists(output_path: str) -> bool:
    partition_path = get_partition_path(output_path)
    if not os.path.exists(partition_path):
        return False
    return (
        any(fname.endswith('.parquet'))
        for fname in os.listdir(partition_path)
    )


def save_data(
    zip_path: str = '',
    data: bytes = None, 
    output_path: str = ''
) -> None:
    partition_path = get_partition_path(output_path)
    create_partition(output_path)

    data_to_read: Any = zip_path if zip_path else io.BytesIO(data)
    
    with zipfile.ZipFile(data_to_read) as zf:
        csv_name = zf.namelist()[0]
        with zf.open(csv_name) as csv_file:
            for chunk in pd.read_csv(
                csv_file, 
                sep=';', 
                encoding='latin1', 
                chunksize=dft_chunk_size,
                dtype=str
            ):
                chunk = (
                    chunk
                        .fillna("")
                        .astype(str)
                        .apply(lambda x: x.str.strip())
                )
                table = pa.Table.from_pandas(
                    chunk, 
                    preserve_index=False
                )
                del chunk
                gc.collect()
                file_name = f'chunk_{uuid.uuid4().hex}.parquet'
                pq.write_table(
                    table,
                    os.path.join(partition_path, file_name),
                    compression='snappy'
                )
                del table
                gc.collect()
        

def main() -> int:
    total_start = time.time()

    for enum in CnpjEnum:
        try:
            start = time.time()

            output_path = f'{base_dir_path}/bronze/{enum.value}/'
            
            if data_already_exists(output_path):
                logger.info(f'[INFO] - {enum.value} already exists - Skipping')
                continue

            url = build_url(enum, dft_year, dft_month)

            zip_path = f'{base_dir_path}/tmp/{uuid.uuid4().hex}.zip'
            fetch_data_stream(url, zip_path)
            save_data(
                zip_path=zip_path, 
                output_path=f'{base_dir_path}/bronze/{enum.value}/'
            )

            os.remove(zip_path)

            end = time.time()
            timed = end - start
            logger.info(f'{enum.value} successfully saved - Time: {timed:.2f} seconds.')

        except Exception as e:
            logger.error(e)

    total_end = time.time()
    timedt = total_end - total_start

    logger.info('Ingestion successfully completed')
    logger.info(f'CNPJ data ingestion total time: {timedt:.2f} seconds.')


if __name__ == '__main__':
    main()
