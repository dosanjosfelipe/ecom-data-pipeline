import pytest
import logging
import pandas as pd
from pathlib import Path
from unittest.mock import patch
from transform.transform_dollar import transform_dollar_data, save_staging_dollar_data


# --- Testes para transform_dollar_data ---
@patch('transform.transform_dollar.pd.read_json')
@patch('transform.transform_dollar.Path.exists')
def test_transform_dollar_data_success(mock_exists, mock_read_json, caplog):
    mock_exists.return_value = True

    raw_data = {
        '@odata.context': 'http://api.teste',
        'value': [
            {
                'cotacaoCompra': 5.00,
                'cotacaoVenda': 5.10,
                'dataHoraCotacao': '2026-03-05 12:00:00'
            }
        ]
    }
    mock_read_json.return_value = pd.DataFrame(raw_data)

    path_input = Path("data/raw/dollar_data.json")

    with caplog.at_level(logging.INFO):
        result_df = transform_dollar_data(path_input)

    assert isinstance(result_df, pd.DataFrame)
    assert 'exchange_rate' in result_df.columns
    assert 'datetime' in result_df.columns
    assert 'cotacaoCompra' not in result_df.columns
    assert '@odata.context' not in result_df.columns
    assert result_df.iloc[0]['exchange_rate'] == 5.10
    assert f'DataFrame criado a partir do arquivo {path_input.name}' in caplog.text


@patch('transform.transform_dollar.Path.exists')
def test_transform_dollar_data_file_not_found(mock_exists):
    mock_exists.return_value = False
    path_errado = Path("arquivo_fantasma.json")

    with pytest.raises(FileNotFoundError) as excinfo:
        transform_dollar_data(path_errado)

    assert f'Arquivo não encontrado: {path_errado}' in str(excinfo.value)


# --- Testes para save_staging_dollar_data ---
@patch('transform.transform_dollar.Path.mkdir')
@patch('pandas.DataFrame.to_parquet')
def test_save_staging_dollar_data_success(mock_to_parquet, mock_mkdir, caplog):
    df_to_save = pd.DataFrame({'col1': [1], 'col2': [2]})

    with caplog.at_level(logging.INFO):
        save_staging_dollar_data(df_to_save)

    mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)

    mock_to_parquet.assert_called_once()

    args, kwargs = mock_to_parquet.call_args
    path_sent_to_pandas = str(args[0])
    assert 'staging' in path_sent_to_pandas
    assert 'dollar_data.parquet' in path_sent_to_pandas
    assert kwargs['index'] is False

    assert 'Arquivo salvo em' in caplog.text
