from flask import Blueprint, request
from typing import Any
from src.queries.database import run


cnpj_bp = Blueprint('cnpj', __name__)


@cnpj_bp.route('/cnpj', methods=['GET'])
def get_cnpj() -> Any:
    year = request.args.get('year', type=int)
    month = request.args.get('month', type=int)
    cnpj_basico = request.args.get('cnpj_basico', type=str)
    result = run('get_cnpj.sql', [year, month, cnpj_basico])
    return result.to_json(orient='records', force_ascii=False)
