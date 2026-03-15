import logging
import sys
sys.path.insert(0, '/opt/airflow/src')
import pandas as pd
from datetime import date, timedelta
from pathlib import Path
from airflow.decorators import dag, task
from extract.extract_api_exchange import extract_exchange_data, save_raw_exchange_data
from extract.extract_db_tables import extract_table_data, save_raw_table_data
from load.load_tables import load_table_data
from transform.transform_exchange import transform_exchange_data, save_staging_exchange_data
from transform.transform_tables import transform_customers_data, save_table_data, transform_order_data, \
    transform_product_data, transform_sales_fact_data
from utils.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


def get_api_url() -> str:
    return ("https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo("
            f"dataInicial='02-01-2026',dataFinalCotacao='{date.today().strftime("%m-%d-%Y")}')?$format=json")


BASE_PATH = Path("/opt/airflow/data")

path_raw = BASE_PATH / "raw"
path_processed = BASE_PATH / "processed"

@dag (
    dag_id='ecommerce_pipeline_etl',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 2,
        'retry_delay':  timedelta(minutes=5)
    },
    description='Pipeline sales with dollar exchange',
    schedule='0 14 * * *',
    catchup=False
)


def ecommerce_pipeline():

    @task
    def extract():
        logging.info('ETAPA 1: EXTRACT\n')

        url = get_api_url()
        exchange_df = extract_exchange_data(url)
        save_raw_exchange_data(exchange_df)
        logging.info('cotação do dólar extraída e salva com sucesso\n')

        customers_df = extract_table_data('customers')
        save_raw_table_data(customers_df, 'customers')
        logging.info('tabela customers extraída e salva com sucesso\n')

        orders_df = extract_table_data('orders')
        save_raw_table_data(orders_df, 'orders')
        logging.info('tabela orders extraída e salva com sucesso\n')

        products_df = extract_table_data('products')
        save_raw_table_data(products_df, 'products')
        logging.info('tabela products extraída e salva com sucesso\n')

        logging.info('ETAPA 1: EXTRACT CONCLUÍDO COM SUCESSO\n')


    @task
    def transform():
        logging.info('ETAPA 2: TRANSFORM\n')

        exchange_transform_df = transform_exchange_data(path_raw / 'exchange_data.json')
        save_staging_exchange_data(exchange_transform_df)
        logging.info('cotação do dólar transformada e salva com sucesso\n')

        transform_customers_df = transform_customers_data(path_raw / 'customers_data.parquet')
        save_table_data(transform_customers_df, 'customers')
        logging.info('tabela customers transformada e salva com sucesso\n')

        transform_date_df = transform_order_data(path_raw / 'orders_data.parquet')
        save_table_data(transform_date_df, 'date')
        logging.info('tabela date transformada e salva com sucesso\n')

        transform_product_df = transform_product_data(path_raw / 'products_data.parquet')
        save_table_data(transform_product_df, 'product')
        logging.info('tabela product transformada e salva com sucesso\n')

        transform_sales_df = transform_sales_fact_data(path_processed / 'date.parquet',
                                                       path_processed / 'exchange.parquet',
                                                       path_raw / 'orders_data.parquet',
                                                       path_processed / 'product.parquet')
        save_table_data(transform_sales_df, 'sales')
        logging.info('tabela sales transformada e salva com sucesso\n')

        logging.info('ETAPA 2: TRANSFORM CONCLUÍDO COM SUCESSO\n')


    @task
    def load():
        logging.info('ETAPA 3: LOAD\n')

        exchange_transform_df = pd.read_parquet(path_processed / 'exchange.parquet')
        load_table_data(exchange_transform_df, 'exchange')
        logging.info('tabela exchange carregada no data warehouse com sucesso\n')

        transform_customers_df = pd.read_parquet(path_processed / 'customers.parquet')
        load_table_data(transform_customers_df, 'customers')
        logging.info('tabela customers carregada no data warehouse com sucesso\n')

        transform_date_df = pd.read_parquet(path_processed / 'date.parquet')
        load_table_data(transform_date_df, 'date_dim')
        logging.info('tabela date_dim carregada no data warehouse com sucesso\n')

        transform_product_df = pd.read_parquet(path_processed / 'product.parquet')
        load_table_data(transform_product_df, 'product')
        logging.info('tabela product carregada no data warehouse com sucesso\n')

        transform_sales_df = pd.read_parquet(path_processed / 'sales.parquet')
        load_table_data(transform_sales_df, 'sales_fact')
        logging.info('tabela sales_fact carregada no data warehouse com sucesso\n')

        logging.info('ETAPA 3: LOAD CONCLUÍDO COM SUCESSO\n')
        logging.info("PIPELINE CONCLUÍDO COM SUCESSO!")


    extract() >> transform() >> load()

ecommerce_pipeline()