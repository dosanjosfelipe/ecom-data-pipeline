import json
import logging
import pandas as pd
import pytest
from unittest.mock import patch

from transform.transform_exchange import (
    transform_exchange_data,
    save_staging_exchange_data
)


def test_transform_exchange_data_file_not_found(tmp_path):
    fake_path = tmp_path / "nao_existe.json"

    with pytest.raises(FileNotFoundError) as exc:
        transform_exchange_data(fake_path)

    assert f"Arquivo não encontrado: {fake_path}" in str(exc.value)


def test_transform_exchange_data_success(tmp_path, caplog):
    fake_path = tmp_path / "exchange.json"

    data = {
        "@odata.context": "context",
        "value": [
            {
                "cotacaoCompra": 5.0,
                "cotacaoVenda": 5.123,
                "dataHoraCotacao": "2024-01-01 10:00:00"
            }
        ]
    }

    with open(fake_path, "w", encoding="utf-8") as f:
        json.dump(data, f)

    with caplog.at_level(logging.INFO):
        result = transform_exchange_data(fake_path)

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert list(result.columns) == ["id", "exchange_rate", "issue_date"]

    assert result.loc[0, "exchange_rate"] == 5.12
    assert result.loc[0, "id"] == 1
    assert result.loc[0, "issue_date"] == "2024-01-01 10:00:00"

    assert f"Dados carregados do arquivo {fake_path.name}" in caplog.text


def test_transform_exchange_data_multiple_rows(tmp_path):
    fake_path = tmp_path / "exchange.json"

    data = {
        "value": [
            {
                "cotacaoCompra": 5.0,
                "cotacaoVenda": 5.123,
                "dataHoraCotacao": "2024-01-01 10:00:00"
            },
            {
                "cotacaoCompra": 5.1,
                "cotacaoVenda": 5.567,
                "dataHoraCotacao": "2024-01-02 10:00:00"
            }
        ]
    }

    with open(fake_path, "w", encoding="utf-8") as f:
        json.dump(data, f)

    result = transform_exchange_data(fake_path)

    assert len(result) == 2
    assert list(result["id"]) == [1, 2]
    assert result.loc[1, "exchange_rate"] == 5.57


def test_transform_exchange_data_missing_value_key(tmp_path):
    fake_path = tmp_path / "exchange.json"

    data = {
        "@odata.context": "context"
    }

    with open(fake_path, "w", encoding="utf-8") as f:
        json.dump(data, f)

    with pytest.raises(ValueError) as exc:
        transform_exchange_data(fake_path)

    assert "value" in str(exc.value)


def test_transform_exchange_data_empty_value(tmp_path):
    fake_path = tmp_path / "exchange.json"

    data = {
        "value": []
    }

    with open(fake_path, "w", encoding="utf-8") as f:
        json.dump(data, f)

    with pytest.raises(ValueError) as exc:
        transform_exchange_data(fake_path)

    assert "vazia" in str(exc.value)


def test_transform_exchange_data_empty_after_normalize(tmp_path):
    fake_path = tmp_path / "exchange.json"

    data = {
        "value": [{}]
    }

    with open(fake_path, "w", encoding="utf-8") as f:
        json.dump(data, f)

    with pytest.raises(ValueError) as exc:
        transform_exchange_data(fake_path)

    assert "DataFrame vazio" in str(exc.value)


# TESTES: save_staging_exchange_data

@patch("transform.transform_exchange.Path.mkdir")
@patch("transform.transform_exchange.pd.DataFrame.to_parquet")
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
    _, kwargs = mock_to_parquet.call_args

    assert kwargs["index"] is False

    assert "Arquivo salvo em" in caplog.text