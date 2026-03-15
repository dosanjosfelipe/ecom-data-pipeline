import logging
import pandas as pd
from unittest.mock import patch

from transform.transform_exchange import transform_exchange_data, save_staging_exchange_data


# --- Testes para transform_exchange_data ---

def test_transform_exchange_data_file_not_found(tmp_path):
    fake_path = tmp_path / "arquivo_inexistente.json"

    try:
        transform_exchange_data(fake_path)
    except FileNotFoundError as e:
        assert f"Arquivo não encontrado: {fake_path}" in str(e)


@patch('transform.transform_exchange.pd.read_json')
def test_transform_exchange_data_success(mock_read_json, tmp_path, caplog):
    fake_path = tmp_path / "exchange.json"

    fake_path.touch()

    mock_df = pd.DataFrame({
        "@odata.context": ["context"],
        "value": [[{
            "cotacaoCompra": 5.0,
            "cotacaoVenda": 5.123,
            "dataHoraCotacao": "2024-01-01 10:00:00"
        }]]
    })

    mock_read_json.return_value = mock_df

    with caplog.at_level(logging.INFO):
        result = transform_exchange_data(fake_path)

    assert isinstance(result, pd.DataFrame)
    assert "id" in result.columns
    assert "exchange_rate" in result.columns
    assert "issue_date" in result.columns
    assert "cotacaoCompra" not in result.columns

    assert result.loc[0, "exchange_rate"] == 5.12
    assert result.loc[0, "id"] == 1

    assert f"DataFrame criado a partir do arquivo {fake_path.name}" in caplog.text


# --- Testes para save_staging_exchange_data ---

@patch('transform.transform_exchange.Path.mkdir')
@patch('transform.transform_exchange.pd.DataFrame.to_parquet')
def test_save_staging_exchange_data(mock_to_parquet, mock_mkdir, caplog):
    df = pd.DataFrame({
        "id": [1],
        "exchange_rate": [5.12],
        "issue_date": ["2024-01-01 10:00:00"]
    })

    with caplog.at_level(logging.INFO):
        save_staging_exchange_data(df)

    mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)

    mock_to_parquet.assert_called_once()
    args, kwargs = mock_to_parquet.call_args

    assert kwargs["index"] is False

    assert "Arquivo salvo em" in caplog.text