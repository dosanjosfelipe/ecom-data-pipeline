from pathlib import Path
import logging
import pandas as pd

logger = logging.getLogger(__name__)

def normalize_dollar_columns(df: pd.DataFrame) -> None:
    df = df.drop(columns='@odata.context')
    df = pd.json_normalize(df['value'])

    df = df.rename(columns={
        'cotacaoVenda': 'exchange_rate',
        'dataHoraCotacao': 'datetime'
    })

    df = df.drop(columns='cotacaoCompra')

    output_path = Path(__file__).parent.parent.parent / 'data' / 'staging' / 'dollar_data.csv'
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(output_path, index=False)

    logger.info(f'Arquivo salvo em {output_path}')
