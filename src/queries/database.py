import duckdb
from pathlib import Path
from typing import Any


conn = duckdb.connect('/app/data/cnpj.duckdb')


def run(query: str = '', params: tuple = ()) -> Any:
    path = Path(__file__).parent / query
    with open(path, 'r') as f:
        query = f.read()
    return conn.execute(query, params).fetchall()


def load_query(name: str) -> str:
    path = Path(__file__).parent / name
    with open(path, 'r') as f:
        return f.read()
