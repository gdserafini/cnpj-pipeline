import requests as req
import zipfile
import io
import os
import time
from datetime import date
from src.utils.logging import logger
import pandas as pd
from app_config.settings import Settings
import pyarrow as pa
import pyarrow.parquet as pq
import gc
import uuid
from typing import Any
import json
import os


TYPE_MAP = {
    "string": pa.string(),
    "int": pa.int32(),
    "double": pa.float64(),
    "timestamp": pa.date32()
}


def build_url(file_name: str, year: int, month: int) -> str:
    url = f'{Settings().BASE_URL}/{year}-{month:02d}/{file_name}.zip'
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
                chunk_size=Settings().DFT_DWLD_CHUNK_SIZE
            ):
                f.write(chunk)


def create_dir(path: str) -> None:
    if path is None or path.strip() == '':
        raise Exception('Invalid path')
    
    os.makedirs(path, exist_ok=True)


def get_partition_path(
    output_path: str, year: int, month: int
) -> str:
    partition_path = os.path.join(
        output_path.rstrip('/'),
        f'year={year}',
        f'month={month}'
    )
    return partition_path


def create_partition(
    output_path: str, year: int, month: int
) -> None:
    os.makedirs(output_path.rstrip('/'), exist_ok=True)
    partition_path = get_partition_path(output_path, year, month)
    os.makedirs(partition_path, exist_ok=True)


def data_already_exists(
    output_path: str, year: int, month: int
) -> bool:
    partition_path = get_partition_path(output_path, year, month)

    if not os.path.exists(partition_path):
        return False
    
    return (
        any(fname.endswith('.parquet'))
        for fname in os.listdir(partition_path)
    )


def get_schema(types: list[list]) -> Any:
    schema = pa.schema(
        [
            (col, TYPE_MAP[dtype])
            for col, dtype in types
        ]
    )
    return schema


def save_data(
    zip_path: str = '',
    data: bytes = None, 
    output_path: str = '',
    columns: list[str] = None,
    types: list[list] = None,
    year: int = 0,
    month: int = 0
) -> None:
    partition_path = get_partition_path(output_path, year, month)
    create_partition(output_path, year, month)

    data_to_read: Any = zip_path if zip_path else io.BytesIO(data)
    
    with zipfile.ZipFile(data_to_read) as zf:
        csv_name = zf.namelist()[0]
        with zf.open(csv_name) as csv_file:
            for chunk in pd.read_csv(
                csv_file, 
                sep=';', 
                encoding='latin1', 
                chunksize=Settings().DFT_CHUNK_SIZE,
                dtype=str,
                header=None,
                names=columns
            ):
                chunk = chunk.replace('', pd.NA).fillna(pd.NA)
                for col, dtype in types:
                    if dtype == 'timestamp':
                        chunk[col] = (
                            pd
                                .to_datetime(
                                    chunk[col], 
                                    format="%Y%m%d", 
                                    errors="coerce")
                                .dt
                                .date
                        )
                    if dtype == 'double':
                        chunk[col] = chunk[col].str.replace(',', '.')
                table = pa.Table.from_pandas(
                    chunk, 
                    preserve_index=False
                )

                del chunk
                gc.collect()

                schema = get_schema(types)
                table = table.cast(schema)

                file_name = f'chunk_{uuid.uuid4().hex}.parquet'
                pq.write_table(
                    table,
                    os.path.join(partition_path, file_name),
                    compression='snappy'
                )

                del table
                gc.collect()
        

def _get_current_year_month() -> tuple[int, int]:
    today_date = date.today()
    year = today_date.year
    month = today_date.month
    return year, month


BDIR = os.path.dirname(os.path.abspath(__file__))


def main() -> int:
    year, month = _get_current_year_month()

    total_start = time.time()

    logger.info('Starting ingestion service.')

    with open(os.path.join(BDIR, 'modelling.json')) as jf:
        dfs = json.load(jf)
    
    for attr in dfs.values():
        try:
            logger.info(f'attr: {attr}')
            
            start = time.time()

            file_name = attr['name']
            logger.info(f'file_name: {file_name}')

            output_path = f'{Settings().BASE_DIR_PATH}/{file_name}/'
            logger.info(f'output path: {output_path}')
            
            if data_already_exists(output_path, year, month):
                logger.info(f'{file_name} already exists - Skipping')
                continue

            url = build_url(file_name, year, month)
            zip_path = f'{Settings().BASE_DIR_PATH}/tmp/{uuid.uuid4().hex}.zip'

            fetch_data_stream(url, zip_path)
            save_data(
                zip_path=zip_path, 
                output_path=output_path,
                columns=attr['columns'],
                types=attr['cast_types'],
                year=year,
                month=month
            )

            os.remove(zip_path)

            end = time.time()
            timed = end - start
            logger.info(f'{file_name} successfully saved - {timed:.2f} seconds.')

        except Exception as e:
            logger.error(e)

    total_end = time.time()
    timedt = total_end - total_start

    logger.info(f'Ingestion successfully completed - {timedt:.2f} seconds.')


if __name__ == '__main__':
    main()
