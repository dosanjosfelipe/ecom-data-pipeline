import logging
import sqlalchemy
import pandas as pd
from pathlib import Path
from sqlalchemy.exc import ArgumentError
from database.connection import create_connection
from utils.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__file__)


def extract_table_data(table: str) -> pd.DataFrame | None:
    engine = create_connection()

    try:
        df = pd.read_sql(table, engine)
    except sqlalchemy.exc.OperationalError:
        logger.error('Não foi possível fazer a conexão com o banco de dados')
        return None
    except sqlalchemy.exc.InterfaceError:
        logger.error('Driver do banco de dados não instalado ou mal configurado')
        return None
    except sqlalchemy.exc.ProgrammingError:
        logger.error(f'Tabela {table} não existe no banco de dados')
        return None

    if df.empty:
        logger.warning('Nenhum dado foi retornado do banco de dados. Dataframe está vazio')

    return df


def save_raw_table_data(df: pd.DataFrame, table_name: str) -> None:
    output_dir = Path(__file__).parent.parent.parent
    output_path = output_dir / 'data' / 'raw' / f'{table_name}_data.parquet'

    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(output_path, index=False)

    relative_path = str(output_path).split("data/", 1)[1]

    logger.info(f'Arquivo salvo em {relative_path}')
