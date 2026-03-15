import logging
import requests
import json
from pathlib import Path
from utils.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


def extract_exchange_data(url: str) -> list:
    try:
        response = requests.get(url, timeout=(3, 10))
        response.raise_for_status()
    except requests.exceptions.Timeout:
        logger.error('Timeout: Nenhuma resposta recebida em 10s')
        return []
    except requests.exceptions.ConnectionError:
        logger.error('Erro de conexão com a API')
        return []
    except requests.exceptions.HTTPError as e:
        logger.error(f'Erro HTTP: {e}')
        return []
    except requests.exceptions.RequestException as e:
        logger.error(f'Erro inesperado na requisição: {e}')
        return []

    data = response.json()

    if not data:
        logger.error('Nenhum dado foi retornado da API')

    return data


def save_raw_exchange_data(data: list) -> None:
    output_dir = Path(__file__).parent.parent.parent
    output_path = output_dir / 'data' / 'raw' / 'exchange_data.json'

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4)

    relative_path = str(output_path).split("data/", 1)[1]

    logger.info(f'Arquivo salvo em {relative_path}')
