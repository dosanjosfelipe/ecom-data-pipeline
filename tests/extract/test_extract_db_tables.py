import logging
import sqlalchemy
import pandas as pd
from unittest.mock import patch, MagicMock
from sqlalchemy.exc import ArgumentError
from extract.extract_db_tables import extract_table_data, save_raw_table_data


# --- Testes para extract_table_data ---
@patch('extract.extract_db_tables.pd.read_sql')
@patch('extract.extract_db_tables.create_connection')
def test_extract_table_data_success(mock_create_connection, mock_read_sql):
    mock_engine = MagicMock()
    mock_create_connection.return_value = mock_engine

    df_dummy = pd.DataFrame({"id": [1, 2], "nome": ["Alice", "Bob"]})
    mock_read_sql.return_value = df_dummy

    table_name = "usuarios"
    result = extract_table_data(table_name)

    pd.testing.assert_frame_equal(result, df_dummy)
    mock_create_connection.assert_called_once()
    mock_read_sql.assert_called_once_with(table_name, mock_engine)


@patch('extract.extract_db_tables.pd.read_sql')
@patch('extract.extract_db_tables.create_connection')
def test_extract_table_data_empty_return(mock_create_connection, mock_read_sql, caplog):
    mock_create_connection.return_value = MagicMock()
    mock_read_sql.return_value = pd.DataFrame()

    with caplog.at_level(logging.WARNING):
        result = extract_table_data("tabela_vazia")

    assert result is not None
    assert result.empty
    assert 'Nenhum dado foi retornado do banco de dados. Dataframe está vazio' in caplog.text


@patch('extract.extract_db_tables.pd.read_sql')
@patch('extract.extract_db_tables.create_connection')
def test_extract_table_data_operational_error(mock_create_connection, mock_read_sql, caplog):
    mock_create_connection.return_value = MagicMock()
    mock_read_sql.side_effect = sqlalchemy.exc.OperationalError("query", "params", Exception("orig"))

    with caplog.at_level(logging.ERROR):
        result = extract_table_data("usuarios")

    assert result is None
    assert 'Não foi possível fazer a conexão com o banco de dados' in caplog.text


@patch('extract.extract_db_tables.pd.read_sql')
@patch('extract.extract_db_tables.create_connection')
def test_extract_table_data_interface_error(mock_create_connection, mock_read_sql, caplog):
    mock_create_connection.return_value = MagicMock()
    mock_read_sql.side_effect = sqlalchemy.exc.InterfaceError("query", "params", Exception("orig"))

    with caplog.at_level(logging.ERROR):
        result = extract_table_data("usuarios")

    assert result is None
    assert 'Driver do banco de dados não instalado ou mal configurado' in caplog.text


@patch('extract.extract_db_tables.pd.read_sql')
@patch('extract.extract_db_tables.create_connection')
def test_extract_table_data_programming_error(mock_create_connection, mock_read_sql, caplog):
    mock_create_connection.return_value = MagicMock()
    mock_read_sql.side_effect = sqlalchemy.exc.ProgrammingError("query", "params", Exception("orig"))

    table_name = "tabela_inexistente"
    with caplog.at_level(logging.ERROR):
        result = extract_table_data(table_name)

    assert result is None
    assert f'Tabela {table_name} não existe no banco de dados' in caplog.text


# --- Testes para save_raw_table_data ---
@patch('extract.extract_db_tables.Path.mkdir')
@patch('pandas.DataFrame.to_parquet')
def test_save_raw_table_data(mock_to_parquet, mock_mkdir, caplog):
    df_dummy = pd.DataFrame({"id": [1], "nome": ["Teste"]})
    table_name = "usuarios"

    with caplog.at_level(logging.INFO):
        save_raw_table_data(df_dummy, table_name)

    mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)

    mock_to_parquet.assert_called_once()

    args, kwargs = mock_to_parquet.call_args
    assert kwargs.get('index') is False
    assert str(args[0]).endswith(f"{table_name}_data.parquet")

    assert 'Arquivo salvo em raw' in caplog.text
    assert f'{table_name}_data.parquet' in caplog.text
