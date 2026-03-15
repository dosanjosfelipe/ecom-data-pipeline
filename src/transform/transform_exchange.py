import logging
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)

def transform_exchange_data(path_name: Path) -> pd.DataFrame:
    if not path_name.exists():
        raise FileNotFoundError(f'Arquivo não encontrado: {path_name}')

    df = pd.read_json(path_name)

    logger.info(f'DataFrame criado a partir do arquivo {path_name.name}')


    df = df.drop(columns='@odata.context')
    df = pd.json_normalize(df['value'])

    df.insert(1, 'id', range(1, len(df) + 1))

    df = df.rename(columns={
        'cotacaoVenda': 'exchange_rate',
        'dataHoraCotacao': 'issue_date'
    })

    df = df.drop(columns='cotacaoCompra')

    df['exchange_rate'] = df['exchange_rate'].round(2)

    return df


def save_staging_exchange_data(df: pd.DataFrame) -> None:
    output_path = Path(__file__).parent.parent.parent / 'data' / 'processed' / 'exchange.parquet'
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(output_path, index=False)

    relative_path = str(output_path).split("data/", 1)[1]

    logger.info(f'Arquivo salvo em {relative_path}')
