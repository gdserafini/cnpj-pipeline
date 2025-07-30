from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from typing import Any
import json
import time
from config.settings import Settings
import os


spark = (
    SparkSession
      .builder
      .appName("cnpj-processing-silver")
      .master("local[*]")
      .getOrCreate()
)


def read_df(path: str, header: str = 'false') -> Any:
    df = (
        spark
        .read
        .option('header', header)
        .option('sep', ',')
        .option('encoding', 'latin1')
        .option('inferSchema', 'false')
        .csv(path)
    )
    return df


def save_df(df: Any, path: str) -> None:
    (
        df
        .write
        .mode('overwrite')
        .parquet(path)
    )


def process_df(
    bpath: str,
    columns: list[str],
    types: list[tuple]
) -> Any:
    df = read_df(bpath)
    df = df.toDF(*columns)
    for (column, dtype) in types:
        if column in df.columns:
            df = df.withColumn(column, col(column).cast(dtype))
        else:
            raise Exception(f'Column {column} not found in dataframe')
    return df


BDIR = os.path.dirname(__file__)
json_path = os.path.join(BDIR, 'silver_modelling.json')

with open(json_path) as f:
  dfs = json.load(f)

names_map = {
  "cnaes": "Cnaes",
  "municipios": "Municipios",
  "naturezas_juridicas": "Naturezas",
  "qualificacoes_de_socios": "Qualificacoes",
  "paises": "Paises",
  "dados_do_simples": "Simples",
  "empresas": "Empresas0",
  "estabelecimentos": "Estabelecimentos0",
  "socios": "Socios0"
}

dft_year = Settings().DFT_YEAR
dft_month_str = Settings().DFT_MONTH_STR
dft_month = Settings().DFT_MONTH
base_dir_path = Settings().BASE_DIR_PATH

total_start = time.time()

for df, attr in dfs.items():
    name = names_map[df]
    file_name = f'{name}-{dft_year}-{dft_month:02d}'
    start = time.time()
    processed_df = process_df(
        f'{base_dir_path}/bronze/{dft_year}/{dft_month_str}/{file_name}.csv',
        attr['columns'],
        attr['cast_types']
    )
    save_df(
        processed_df,
        f'{base_dir_path}/silver/{dft_year}/{dft_month_str}/{file_name}.parquet'
    )
    end = time.time()
    timed = end - start
    print(f'[INFO] - {file_name} saved - {timed:.4f} seconds.')

total_end = time.time()
timedt = total_end - total_start

print('[INFO] - Silver data processing successfully completed')
print(f'[INFO] - Total time: {timedt:.4f} seconds.')
