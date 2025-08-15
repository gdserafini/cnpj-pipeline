from enum import Enum
from pyspark.sql import SparkSession
from config.settings import Settings
from src.utils.logging import logger


spark = (
    SparkSession
        .builder
        .master('local[*]')
        .getOrCreate()
)


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


BASE_DIR = Settings().BASE_DIR_PATH


def main():
    for enum in CnpjEnum:
        df = spark.read.parquet(f'{BASE_DIR}/bronze/{enum.value}')


if __name__ == '__main__':
    main()
