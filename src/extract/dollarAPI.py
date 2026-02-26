from pathlib import Path
import requests
import logging
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def extract_dollar_data(url:str) -> list:
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

    with open(output_path, 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4)

    logger.info(f'Arquivo salvo em {output_path}')
    return data
