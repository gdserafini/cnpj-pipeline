from enum import Enum
import requests as req
import zipfile
import io
import os
import sys
import time
import logging


dft_year = int(os.getenv('DFT_YEAR'))
dft_month = int(os.getenv('DFT_MONTH'))
dft_month_str = os.getenv('DFT_MONTH_STR')
BASE_URL = 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj'
base_dir_path = '../../data'


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


def save_bytes_data(data: bytes, path: str) -> None:
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        csv_name = zf.namelist()[0]
        with zf.open(csv_name) as csv_file:
            content = csv_file.read()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'wb') as f:
        f.write(content)


total_start = time.time()

tmp_count = 0

for enum in CnpjEnum:
    start = time.time()
    url = build_url(enum, dft_year, dft_month)
    data = request_data(url)
    file_name = f'{enum.value}-{dft_year}-{dft_month:02d}.csv'
    save_bytes_data(
        data, 
        f'{base_dir_path}/bronze/{dft_year}/{dft_month_str}/{file_name}'
    )
    end = time.time()
    print(f'[INFO] - {file_name} successfully saved - Time: {(end - start):.2f} seconds.')
    tmp_count += 1
    if tmp_count == 2: break

total_end = time.time()

print('[INFO] - Ingestion successfully completed')
print(f'[INFO] - CNPJ data ingestion total time: {(total_end - total_start):.2f} seconds.')
