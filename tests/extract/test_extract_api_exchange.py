import requests
import logging
from unittest.mock import patch, mock_open, MagicMock

from extract.extract_api_dollar import extract_dollar_data, save_raw_dollar_data


# --- Testes para extract_dollar_data ---
@patch('extract.extract_api_dollar.requests.get')
def test_extract_dollar_data_success(mock_get):
    mock_response = MagicMock()
    mock_response.json.return_value = [{"cotacaoCompra": 5.0, "cotacaoVenda": 5.1}]
    mock_get.return_value = mock_response

    url = "https://api-falsa.com/dolar"
    result = extract_dollar_data(url)

    assert result == [{"cotacaoCompra": 5.0, "cotacaoVenda": 5.1}]
    mock_get.assert_called_once_with(url, timeout=(3, 10))
    mock_response.raise_for_status.assert_called_once()


@patch('extract.extract_api_dollar.requests.get')
def test_extract_dollar_data_timeout(mock_get, caplog):
    mock_get.side_effect = requests.exceptions.Timeout("Timeout forced")

    with caplog.at_level(logging.ERROR):
        result = extract_dollar_data("https://api-falsa.com/dolar")

    assert result == []
    assert 'Timeout: Nenhuma resposta recebida em 10s' in caplog.text


@patch('extract.extract_api_dollar.requests.get')
def test_extract_dollar_data_connection_error(mock_get, caplog):
    mock_get.side_effect = requests.exceptions.ConnectionError("Connection failed")

    with caplog.at_level(logging.ERROR):
        result = extract_dollar_data("https://api-falsa.com/dolar")

    assert result == []
    assert 'Erro de conexão com a API' in caplog.text


@patch('extract.extract_api_dollar.requests.get')
def test_extract_dollar_data_http_error(mock_get, caplog):
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")
    mock_get.return_value = mock_response

    with caplog.at_level(logging.ERROR):
        result = extract_dollar_data("https://api-falsa.com/dolar")

    assert result == []
    assert 'Erro HTTP: 404 Not Found' in caplog.text


@patch('extract.extract_api_dollar.requests.get')
def test_extract_dollar_data_empty_return(mock_get, caplog):
    mock_response = MagicMock()
    mock_response.json.return_value = []
    mock_get.return_value = mock_response

    with caplog.at_level(logging.ERROR):
        result = extract_dollar_data("https://api-falsa.com/dolar")

    assert result == []
    assert 'Nenhum dado foi retornado da API' in caplog.text


# --- Testes para save_raw_dollar_data ---
@patch('extract.extract_api_dollar.Path.mkdir')
@patch('builtins.open', new_callable=mock_open)
@patch('extract.extract_api_dollar.json.dump')
def test_save_raw_dollar_data(mock_json_dump, mock_file_open, mock_mkdir, caplog):
    dummy_data = [{"cotacaoCompra": 5.0, "cotacaoVenda": 5.1}]

    with caplog.at_level(logging.INFO):
        save_raw_dollar_data(dummy_data)

    mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)

    mock_file_open.assert_called_once()
    args, kwargs = mock_file_open.call_args
    assert args[1] == 'w'
    assert kwargs['encoding'] == 'utf-8'

    mock_json_dump.assert_called_once()
    assert mock_json_dump.call_args[0][0] == dummy_data
    assert mock_json_dump.call_args[1]['indent'] == 4

    assert 'Arquivo salvo em' in caplog.text