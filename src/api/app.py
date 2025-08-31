from flask import Flask, request
from typing import Any
from src.queries.database import load_query, conn


app = Flask(__name__)


@app.route('/cnaes', methods=['GET'])
def read_cnaes() -> Any:
    year = request.args.get('year', type=int)
    month = request.args.get('month', type=int)
    query = load_query('get_cnaes.sql')
    result = conn.execute(query, [year, month]).fetchdf()
    return result.to_json(orient='records', force_ascii=False)


if __name__ == "__main__":
    app.run(
        host="0.0.0.0", 
        port=5000, 
        debug=True, 
        use_reloader=False
    )
