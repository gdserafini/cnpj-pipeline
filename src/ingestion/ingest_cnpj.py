from enum import Enum
import requests as req
import zipfile
import io
import os
import sys
import time
from src.utils.logging import logger
import pandas as pd
from config.settings import Settings


dft_year = Settings().DFT_YEAR
dft_month = Settings().DFT_MONTH
dft_month_str = Settings().DFT_MONTH_STR
base_dir_path = Settings().BASE_DIR_PATH
base_url = Settings().BASE_URL
dft_chunk_size = Settings().DFT_CHUNK_SIZE


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


def request_data(url: str) -> bytes:
    response = req.get(url)
    response.raise_for_status()
    return response.content


def save_bytes_data(data: bytes, path: str):
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        csv_name = zf.namelist()[0]
        with zf.open(csv_name) as csv_file:
            reader = pd.read_csv(
                csv_file, 
                sep=';', 
                encoding='latin1', 
                dtype=str, 
                chunksize=dft_chunk_size
            )
            os.makedirs(os.path.dirname(path), exist_ok=True)
            iter_cout = 0
            for chunk in reader:
                chunk.to_csv(
                    path,
                    mode='w' if iter_cout == 0 else 'a',
                    header=iter_cout == 0,
                    index=False
                )
                iter_cout += 1


total_start = time.time()
for enum in CnpjEnum:
    try:
        start = time.time()
        url = build_url(enum, dft_year, dft_month)
        data = request_data(url)
        file_name = f'{enum.value}-{dft_year}-{dft_month:02d}.csv'
        save_bytes_data(
            data, 
            f'{base_dir_path}/bronze/{dft_year}/{dft_month_str}/{file_name}'
        )
        end = time.time()
        timed = end - start
        logger.info(f'{file_name} successfully saved - Time: {timed:.2f} seconds.')
    except Exception as e:
        logger.error(e)

total_end = time.time()
timedt = total_end - total_start
logger.info('Ingestion successfully completed')
logger.info(f'CNPJ data ingestion total time: {timedt:.2f} seconds.')
