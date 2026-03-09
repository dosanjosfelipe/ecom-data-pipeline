import logging
import pandas as pd
from pathlib import Path
from utils.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


def transform_dollar_data(path_name: Path) -> pd.DataFrame:
    if not path_name.exists():
        raise FileNotFoundError(f'Arquivo não encontrado: {path_name}')

    df = pd.read_json(path_name)

    logger.info(f'DataFrame criado a partir do arquivo {path_name.name}')

    df = df.drop(columns='@odata.context')
    df = pd.json_normalize(df['value'])

    df = df.rename(columns={
        'cotacaoVenda': 'exchange_rate',
        'dataHoraCotacao': 'datetime'
    })

    df = df.drop(columns='cotacaoCompra')

    return df


def save_staging_dollar_data(df: pd.DataFrame) -> None:
    output_path = Path(__file__).parent.parent.parent / 'data' / 'staging' / 'dollar_data.parquet'
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(output_path, index=False)

    relative_path = str(output_path).split("data/", 1)[1]

    logger.info(f'Arquivo salvo em {relative_path}')
