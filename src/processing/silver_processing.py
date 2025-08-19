from enum import Enum
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, floor
import pyspark.sql.functions as F
from config.settings import Settings
from src.utils.logging import logger
import json
from typing import Any
import os
import gc


spark = (
    SparkSession
        .builder
        .appName('processing')
        .master('local[*]')
        .config("spark.network.timeout", "1200s")
        .config("spark.executor.heartbeatInterval", "120s")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
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
dft_year = Settings().DFT_YEAR
dft_month = Settings().DFT_MONTH


def read_df(path: str) -> Any:
    df = (
        spark
        .read
        .parquet(path)
        .filter(
            (col('year') == dft_year) &
            (col('month') == dft_month)
        )
    )
    logger.info(f'DF readed - {path}')
    return df


def save_df(df: Any, path: str) -> None:
    (
        df
            .write
            .mode('overwrite')
            .option('maxRecordsPerFile', Settings().DFT_CHUNK_SIZE)
            .option('compression', 'snappy')
            .partitionBy('year', 'month')
            .parquet(path)
    )
    logger.info(f'DF saved - {path}')


def get_partition_path(output_path: str) -> str:
    partition_path = os.path.join(
        output_path.rstrip('/'),
        f'year={dft_year}',
        f'month={dft_month}'
    )
    return partition_path


def data_already_exists(output_path: str) -> bool:
    partition_path = get_partition_path(output_path)
    if not os.path.exists(partition_path):
        return False
    return (
        any(fname.endswith('.parquet'))
        for fname in os.listdir(partition_path)
    )


def get_npartitions(path: str):
    file_name = path.split('/')[-3]
    if file_name == 'Empresas0': return 250
    elif file_name == 'Estabelecimentos0': return 300
    elif file_name == 'Socios0': return 250
    else: return 1


def process_df(
    path: str,
    columns: list[str],
    types: list[tuple]
) -> Any:
    df = read_df(path)
    df = df.toDF(*columns)
    logger.info(f'Columns renamed - {path}')
    for (column, dtype) in types:
        if column in df.columns:
            df = df.withColumn(column, col(column).cast(dtype))
            logger.info(f'Columns casted - {path}')
        else:
            raise Exception(f'Column {column} not found in dataframe')
    n_partitions = get_npartitions(path)
    if n_partitions > 1:
        df = df.repartition(n_partitions, 'year', 'month')
        logger.info(f'DF repartitioned - {path}')
    return df


def main():
    with open('./src/processing/silver_modelling.json') as jf:
        dfs = json.load(jf)

    for df, attr in dfs.items():
        df_name = attr['name']
        bpath = f'{BASE_DIR}/bronze/{df_name}'
        spath = f'{BASE_DIR}/silver/{df_name}/'

        if data_already_exists(spath):
          logger.info(f'{df_name} already exists, skipping...')
          continue

        processed_df = process_df(
            bpath,
            attr['columns'],
            attr['cast_types']
        )
        save_df(processed_df, spath)        
        del processed_df
        gc.collect()
        logger.info(f'{df_name} successfully saved')


if __name__ == '__main__':
    main()
