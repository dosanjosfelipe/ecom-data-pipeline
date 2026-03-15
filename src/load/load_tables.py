import logging
import pandas as pd
from database.connection import create_wh_connection

logger = logging.getLogger(__file__)

def load_table_data(df: pd.DataFrame, table_name: str) -> None:
    engine = create_wh_connection()

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",
        index=False,
    )

    logger.info('Dados carregados com sucesso \n')

    df_check = pd.read_sql(f'SELECT * FROM {table_name}',con=engine)
    logger.info(f'Total de registros na tabela: {len(df_check)}')