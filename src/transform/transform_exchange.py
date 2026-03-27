import logging
import json
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)


def transform_exchange_data(path_name: Path) -> pd.DataFrame:
    if not path_name.exists():
        raise FileNotFoundError(f'Arquivo não encontrado: {path_name}')

    with open(path_name, 'r', encoding='utf-8') as f:
        data = json.load(f)

    logger.info(f'Dados carregados do arquivo {path_name.name}')

    if 'value' not in data:
        raise ValueError("Chave 'value' não encontrada no JSON")

    if not data['value']:
        raise ValueError("Lista 'value' está vazia")

    df = pd.json_normalize(data['value'])

    if df.empty:
        raise ValueError("DataFrame vazio após transformação")

    df.insert(0, 'id', range(1, len(df) + 1))

    df = df.rename(columns={
        'cotacaoVenda': 'exchange_rate',
        'dataHoraCotacao': 'issue_date'
    })

    if 'cotacaoCompra' in df.columns:
        df = df.drop(columns='cotacaoCompra')

    df['exchange_rate'] = df['exchange_rate'].round(2)

    return df


def save_staging_exchange_data(df: pd.DataFrame) -> None:
    output_path = Path(__file__).parent.parent.parent / 'data' / 'processed' / 'exchange.parquet'
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(output_path, index=False)

    relative_path = str(output_path).split("data/", 1)[1]

    logger.info(f'Arquivo salvo em {relative_path}')


