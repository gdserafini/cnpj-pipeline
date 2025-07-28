from enum import Enum
import requests as req
import zipfile
import io
import os
import sys
import time
from src.utils.logging import logger
import pandas as pd


dft_year = int(os.getenv('DFT_YEAR'))
dft_month = int(os.getenv('DFT_MONTH'))
dft_month_str = os.getenv('DFT_MONTH_STR')
base_dir_path = 'data'
BASE_URL = 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj'
DFT_CHUNK_SIZE = int(os.getenv('DFT_CHUNK_SIZE'))


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
    url = f'{BASE_URL}/{year}-{month:02d}/{data.value}.zip'
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
                chunksize=DFT_CHUNK_SIZE
            )
            os.makedirs(os.path.dirname(path), exist_ok=True)
            for i, chunk in enumerate(reader):
                mode = 'w' if i == 0 else 'a'
                header = i == 0
                chunk.to_csv(
                    path, 
                    mode=mode, 
                    header=header, 
                    index=False
                )


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
        logger.info(
            f'{file_name} successfully saved - Time: {(end - start):.2f} seconds.'
        )
    except Exception as e:
        logger.error(e)

total_end = time.time()
logger.info('Ingestion successfully completed')
logger.info(
    f'CNPJ data ingestion total time: {(total_end - total_start):.2f} seconds.'
)
