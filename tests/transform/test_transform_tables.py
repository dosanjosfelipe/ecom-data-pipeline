import pytest
import logging
import pandas as pd
from pathlib import Path
from unittest.mock import patch

from transform.transform_tables import (
    transform_customers_data,
    transform_order_data,
    transform_product_data,
    transform_sales_fact_data,
    save_table_data
)


# transform_customers_data
@patch("transform.transform_tables.pd.read_parquet")
@patch("transform.transform_tables.Path.exists")
def test_transform_customers_data_success(mock_exists, mock_read_parquet, caplog):
    mock_exists.return_value = True

    df_raw = pd.DataFrame({
        "id": [1],
        "name": ["Felipe"],
        "email": ["teste@email.com"],
        "phone": ["999999"],
        "state": ["SP"]
    })

    mock_read_parquet.return_value = df_raw
    path_input = Path("data/raw/customers.parquet")

    with caplog.at_level(logging.INFO):
        df = transform_customers_data(path_input)

    assert isinstance(df, pd.DataFrame)
    assert "name" not in df.columns
    assert "email" not in df.columns
    assert "phone" not in df.columns
    assert df.iloc[0]["region"] == "southeast"
    assert f"DataFrame criado a partir do arquivo {path_input.name}" in caplog.text


@patch("transform.transform_tables.Path.exists")
def test_transform_customers_data_file_not_found(mock_exists):
    mock_exists.return_value = False

    path = Path("arquivo_inexistente.parquet")

    with pytest.raises(FileNotFoundError):
        transform_customers_data(path)


# transform_order_data
@patch("transform.transform_tables.pd.read_parquet")
@patch("transform.transform_tables.Path.exists")
def test_transform_order_data_success(mock_exists, mock_read_parquet, caplog):

    mock_exists.return_value = True

    df_raw = pd.DataFrame({
        "id": [1],
        "customer_id": [10],
        "product_id": [2],
        "quantity": [3],
        "order_date": pd.to_datetime(["2026-03-01"])
    })

    mock_read_parquet.return_value = df_raw
    path_input = Path("orders.parquet")

    with caplog.at_level(logging.INFO):
        df = transform_order_data(path_input)

    assert isinstance(df, pd.DataFrame)
    assert "customer_id" not in df.columns
    assert "product_id" not in df.columns
    assert "quantity" not in df.columns
    assert "day" in df.columns
    assert "month" in df.columns
    assert "year" in df.columns
    assert "sale_date" in df.columns
    assert f"DataFrame criado a partir do arquivo {path_input.name}" in caplog.text


@patch("transform.transform_tables.Path.exists")
def test_transform_order_data_file_not_found(mock_exists):
    mock_exists.return_value = False

    path = Path("orders.parquet")

    with pytest.raises(FileNotFoundError):
        transform_order_data(path)


# transform_product_data
@patch("transform.transform_tables.pd.read_parquet")
def test_transform_product_data_success(mock_read_parquet, caplog):

    df_raw = pd.DataFrame({
        "id": [1],
        "sale_price": [100],
        "unit_coust": [70]
    })

    mock_read_parquet.return_value = df_raw
    path_input = Path("products.parquet")

    with caplog.at_level(logging.INFO):
        df = transform_product_data(path_input)

    assert "unit_price" in df.columns
    assert "unit_cost" in df.columns
    assert "sale_price" not in df.columns
    assert "unit_coust" not in df.columns
    assert f"DataFrame criado a partir do arquivo {path_input.name}" in caplog.text


# transform_sales_fact_data
@patch("transform.transform_tables.pd.read_parquet")
@patch("transform.transform_tables.Path.exists")
def test_transform_sales_fact_data_success(mock_exists, mock_read_parquet):

    mock_exists.return_value = True

    date_df = pd.DataFrame({
        "id": [1],
        "sale_date": pd.to_datetime(["2026-03-01"])
    })

    exchange_df = pd.DataFrame({
        "id": [1],
        "issue_date": pd.to_datetime(["2026-02-28"]),
        "exchange_rate": [5.0]
    })

    order_df = pd.DataFrame({
        "id": [1],
        "customer_id": [10],
        "product_id": [2],
        "quantity": [1],
        "order_date": pd.to_datetime(["2026-03-01"])
    })

    product_df = pd.DataFrame({
        "id": [2],
        "unit_price": [100],
        "unit_cost": [60]
    })

    mock_read_parquet.side_effect = [
        date_df,
        exchange_df,
        order_df,
        product_df
    ]

    result = transform_sales_fact_data(
        Path("date.parquet"),
        Path("exchange.parquet"),
        Path("orders.parquet"),
        Path("products.parquet")
    )

    assert isinstance(result, pd.DataFrame)
    assert "gross_revenue_brl" in result.columns
    assert "net_revenue_brl" in result.columns
    assert "gross_revenue_usd" in result.columns
    assert "net_revenue_usd" in result.columns

    assert result.iloc[0]["gross_revenue_brl"] == 100
    assert result.iloc[0]["net_revenue_brl"] == 40


@patch("transform.transform_tables.Path.exists")
def test_transform_sales_fact_data_file_not_found(mock_exists):
    mock_exists.return_value = False

    with pytest.raises(FileNotFoundError):
        transform_sales_fact_data(
            Path("date.parquet"),
            Path("exchange.parquet"),
            Path("orders.parquet"),
            Path("products.parquet")
        )


# save_table_data
@patch("transform.transform_tables.Path.mkdir")
@patch("pandas.DataFrame.to_parquet")
def test_save_table_data_success(mock_to_parquet, mock_mkdir, caplog):

    df = pd.DataFrame({"col": [1]})

    with caplog.at_level(logging.INFO):
        save_table_data(df, "customers")

    mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
    mock_to_parquet.assert_called_once()

    args, kwargs = mock_to_parquet.call_args

    path_sent = str(args[0])
    assert "processed" in path_sent
    assert "customers.parquet" in path_sent
    assert kwargs["index"] is False

    assert "Arquivo salvo em" in caplog.text
