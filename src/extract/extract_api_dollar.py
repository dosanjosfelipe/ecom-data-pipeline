import logging
from pathlib import Path
import pandas as pd
import requests
import json

logger = logging.getLogger(__name__)
def extract_dollar_data(url: str) -> list:
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
        return []

    output_dir = Path(__file__).parent.parent.parent
    output_path = output_dir / 'data' / 'raw' / 'dollar_data.json'
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4)

    logger.info(f'Arquivo salvo em {output_path}')
    return data

def create_dataframe(path_name: Path) -> pd.DataFrame:
    if not path_name.exists():
        raise FileNotFoundError(f'Arquivo não encontrado: {path_name}')

    df = pd.read_json(path_name)

    logger.info(f'DataFrame criado a partir do arquivo {path_name.name}')
    return df