import logging
import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

logger = logging.getLogger(__file__)

env_path = Path(__file__).resolve().parent.parent.parent / 'config' / '.env'
load_dotenv(env_path)

user = os.getenv('user')
password = os.getenv('password')
database = os.getenv('database')
warehouse = os.getenv('warehouse')
host = 'host.docker.internal'

DATABASE_URL = f'postgresql+psycopg2://{user}:{password}@{host}:5432/{database}'
WAREHOUSE_URL = f'postgresql+psycopg2://{user}:{password}@{host}:5432/{warehouse}'

def create_db_connection() -> Engine:
    engine = create_engine(DATABASE_URL)
    logger.info(f'Banco de dados conectado em {DATABASE_URL.split('@')[1]}')

    return engine

def create_wh_connection() -> Engine:
    engine = create_engine(WAREHOUSE_URL)
    logger.info(f'Banco de dados conectado em {DATABASE_URL.split('@')[1]}')

    return engine