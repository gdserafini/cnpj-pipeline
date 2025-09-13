import duckdb
from pathlib import Path
from typing import Any


conn = duckdb.connect('/app/data/cnpj.duckdb')


def run(
    query_name: str = '', 
    params: tuple = (),
    get_result: bool = True
) -> Any:
    query = load_query(query_name)
    return (
        conn.execute(query, params).fetchdf() if get_result
        else conn.execute(query, params)
    )


def load_query(query_name: str) -> str:
    path = Path(__file__).parent / query_name
    with open(path, 'r') as f:
        return f.read()
