import logging
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__file__)


def transform_customers_data(path_name: Path) -> pd.DataFrame:
    if not path_name.exists():
        raise FileNotFoundError(f'Arquivo não encontrado: {path_name}')

    df = pd.read_parquet(path_name)

    logger.info(f'DataFrame criado a partir do arquivo {path_name.name}')

    df = df.drop(columns=['name', 'email', 'phone'])

    north = ('AC', 'AM', 'RO', 'RR', 'AP', 'PA', 'TO')
    northeast = ('MA', 'PI', 'CE', 'RN', 'PB', 'PE', 'AL', 'SE', 'BA')
    midwest = ('MT', 'MS', 'GO', 'DF')
    southeast = ('SP', 'MG', 'ES', 'RJ')
    south = ('PR', 'SC', 'RS')

    df.loc[df['state'].isin(north), 'region'] = 'north'
    df.loc[df['state'].isin(northeast), 'region'] = 'northeast'
    df.loc[df['state'].isin(midwest), 'region'] = 'midwest'
    df.loc[df['state'].isin(southeast), 'region'] = 'southeast'
    df.loc[df['state'].isin(south), 'region'] = 'south'

    return df


def transform_order_data(path_name: Path) -> pd.DataFrame:
    if not path_name.exists():
        raise FileNotFoundError(f'Arquivo não encontrado: {path_name}')

    df = pd.read_parquet(path_name)

    logger.info(f'DataFrame criado a partir do arquivo {path_name.name}')

    df = df.drop(columns=['customer_id', 'product_id', 'quantity'])

    df['day'] = df['order_date'].dt.day
    df['month'] = df['order_date'].dt.month
    df['month_name'] = df['order_date'].dt.month_name()
    df['day_name'] = df['order_date'].dt.day_name()
    df['is_end_of_week'] = df['order_date'].dt.dayofweek >= 5
    df['quarter'] = df['order_date'].dt.quarter
    df['year'] = df['order_date'].dt.year

    cols = [c for c in df.columns if c != "id"]
    df = df.drop_duplicates(subset=cols)
    df["id"] = range(1, len(df) + 1)

    df = df.rename(columns={'order_date': 'sale_date'})

    return df


def transform_product_data(path_name: Path) -> pd.DataFrame:
    df = pd.read_parquet(path_name)

    logger.info(f'DataFrame criado a partir do arquivo {path_name.name}')

    df = df.rename(columns={'sale_price': 'unit_price', 'unit_coust': 'unit_cost'})

    return df


def transform_sales_fact_data(
        date_path: Path, exchange_path: Path, orders_path: Path, product_path: Path) -> pd.DataFrame:
    if not date_path.exists() or not exchange_path.exists() or not orders_path.exists():
        raise FileNotFoundError('Um dos arquivos não foi encontrado')

    date_df = pd.read_parquet(date_path)
    exchange_df = pd.read_parquet(exchange_path)
    order_df = pd.read_parquet(orders_path)
    product_df = pd.read_parquet(product_path)

    date_df['sale_date'] = pd.to_datetime(date_df['sale_date']).astype('datetime64[ns]')
    order_df['order_date'] = pd.to_datetime(order_df['order_date']).astype('datetime64[ns]')

    date_df = date_df.rename(columns={'id': 'date_id'})

    sales_df = order_df.merge(
        date_df[['date_id', 'sale_date']],
        left_on='order_date',
        right_on='sale_date',
        how='left'
    )

    sales_df['product_id'] = order_df['product_id']
    sales_df['customer_id'] = order_df['customer_id']
    sales_df['sales_quantity'] = order_df['quantity']

    sales_df['sale_date'] = pd.to_datetime(sales_df['sale_date']).astype('datetime64[ns]')
    exchange_df['issue_date'] = pd.to_datetime(exchange_df['issue_date']).astype('datetime64[ns]')

    exchange_df = exchange_df.sort_values('issue_date')
    sales_df = sales_df.sort_values('sale_date')

    exchange_df = exchange_df.rename(columns={'id': 'exchange_id'})

    sales_df = pd.merge_asof(
        sales_df,
        exchange_df[['exchange_id', 'issue_date']],
        left_on='sale_date',
        right_on='issue_date',
        direction='backward'
    )

    sales_df = sales_df.drop(columns=['issue_date'])
    sales_df = sales_df.drop(columns=['sale_date'])

    product_price = product_df.set_index('id')['unit_price']
    product_cost = product_df.set_index('id')['unit_cost']
    exchange_rate = exchange_df.set_index('exchange_id')['exchange_rate']

    margin = product_price - product_cost

    sales_df['gross_revenue_brl'] = (sales_df['product_id'].map(product_price) *
                                     sales_df['sales_quantity']
                                     )

    sales_df['net_revenue_brl'] = (sales_df['product_id'].map(margin) *
                                   sales_df['sales_quantity']
                                   )

    sales_df['gross_revenue_usd'] = ((sales_df['product_id'].map(product_price) /
                                      sales_df['exchange_id'].map(exchange_rate)) *
                                     sales_df['sales_quantity'])

    sales_df['net_revenue_usd'] = ((sales_df['product_id'].map(margin) /
                                    sales_df['exchange_id'].map(exchange_rate)) *
                                   sales_df['sales_quantity'])

    sales_df = sales_df.drop(columns=['order_date', 'id', 'sales_quantity'])
    sales_df = sales_df.sort_values(['date_id', 'product_id'])
    sales_df['gross_revenue_usd'] = sales_df['gross_revenue_usd'].round(2)
    sales_df['net_revenue_usd'] = sales_df['net_revenue_usd'].round(2)
    sales_df = sales_df.sort_values(['date_id', 'product_id'])

    return sales_df


def save_table_data(df: pd.DataFrame, table_name: str) -> None:
    output_path = Path(__file__).parent.parent.parent / 'data' / 'processed' / f'{table_name}.parquet'
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(output_path, index=False)

    relative_path = str(output_path).split("data/", 1)[1]

    logger.info(f'Arquivo salvo em {relative_path}')
