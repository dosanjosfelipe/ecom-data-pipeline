from unittest.mock import patch, MagicMock, mock_open
from requests.exceptions import Timeout, HTTPError
from src.extract.extract_api_dollar import extract_dollar_data


# ---------------------------
# Teste de sucesso
# ---------------------------
@patch("src.extract.dollarAPI.requests.get")
@patch("builtins.open", new_callable=mock_open)
def test_extract_dollar_data_success(mock_file, mock_get):
    mock_response = MagicMock()
    mock_response.json.return_value = {"value": [{"cotacaoCompra": 5.10}]}
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    result = extract_dollar_data("http://fake-url")

    assert result == {"value": [{"cotacaoCompra": 5.10}]}
    mock_get.assert_called_once()
    mock_file.assert_called_once()


# ---------------------------
# Teste Timeout
# ---------------------------
@patch("src.extract.dollarAPI.requests.get", side_effect=Timeout)
def test_extract_dollar_data_timeout(mock_get):
    result = extract_dollar_data("http://fake-url")

    assert result == []
    mock_get.assert_called_once_with("http://fake-url", timeout=(3, 10))


# ---------------------------
# Teste HTTPError
# ---------------------------
@patch("src.extract.dollarAPI.requests.get")
def test_extract_dollar_data_http_error(mock_get):
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = HTTPError("404 Error")
    mock_get.return_value = mock_response

    result = extract_dollar_data("http://fake-url")
    assert result == []


# ---------------------------
# Teste retorno vazio
# ---------------------------
@patch("src.extract.dollarAPI.requests.get")
def test_extract_dollar_data_empty_response(mock_get):
    mock_response = MagicMock()
    mock_response.json.return_value = {}
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    result = extract_dollar_data("http://fake-url")
    assert result == []