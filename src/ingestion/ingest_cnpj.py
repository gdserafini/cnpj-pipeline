import requests as req
import zipfile
import io
import os
import time
from src.utils.logging import logger
import pandas as pd
from config.settings import Settings
import pyarrow as pa
import pyarrow.parquet as pq
import gc
import uuid
from typing import Any
import json


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


def get_partition_path(output_path: str) -> str:
    partition_path = os.path.join(
        output_path.rstrip('/'),
        f'year={Settings().DFT_YEAR}',
        f'month={Settings().DFT_MONTH:02d}'
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
    types: list[list] = None
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
        

def main() -> int:
    total_start = time.time()

    logger.info('Starting ingestion service.')

    with open('./src/ingestion/modelling.json') as jf:
        dfs = json.load(jf)

    for attr in dfs.values():
        try:
            start = time.time()

            file_name = attr['name']
            output_path = f'{Settings().BASE_DIR_PATH}/{file_name}/'
            
            if data_already_exists(output_path):
                logger.info(f'{file_name} already exists - Skipping')
                continue

            url = build_url(
                file_name, 
                Settings().DFT_YEAR, 
                Settings().DFT_MONTH
            )
            zip_path = f'{Settings().BASE_DIR_PATH}/tmp/{uuid.uuid4().hex}.zip'

            fetch_data_stream(url, zip_path)
            save_data(
                zip_path=zip_path, 
                output_path=output_path,
                columns=attr['columns'],
                types=attr['cast_types']
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
