import pandas as pd
import pytest
import requests

from src.extract import APIExtractor
from tests.conftest import FakeResponse


def test_get_dataset_catalog_with_keyword_filter():
    extractor = APIExtractor("https://example.com/api", fail_fast=True)

    pages = {
        1: {
            "_meta": {"pageCount": 2},
            "data": [
                {"id": "1", "judul": "Produksi Padi Jawa Tengah"},
                {"id": "2", "judul": "Data Perikanan"},
            ],
        },
        2: {
            "_meta": {"pageCount": 2},
            "data": [{"id": "3", "judul": "Luas Panen Padi"}],
        },
    }

    def fake_get(url, timeout):
        page = int(url.split("page=")[1])
        return FakeResponse(pages[page])

    extractor.session.get = fake_get

    df = extractor.get_dataset_catalog(target_keywords=["padi"])

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert set(df["id"].tolist()) == {"1", "3"}


def test_get_dataset_details_adds_dataset_id():
    extractor = APIExtractor("https://example.com/api", fail_fast=True)

    pages = {
        1: {"_meta": {"pageCount": 2}, "data": [{"value": 10}, {"value": 20}]},
        2: {"_meta": {"pageCount": 2}, "data": [{"value": 30}]},
    }

    def fake_get(url, timeout):
        page = int(url.split("page=")[1])
        return FakeResponse(pages[page])

    extractor.session.get = fake_get

    df = extractor.get_dataset_details("dataset-123")

    assert len(df) == 3
    assert "dataset_id" in df.columns
    assert set(df["dataset_id"].unique()) == {"dataset-123"}


def test_fail_fast_false_returns_partial_data():
    extractor = APIExtractor("https://example.com/api", fail_fast=False)

    def fake_get(url, timeout):
        page = int(url.split("page=")[1])
        if page == 1:
            return FakeResponse(
                {"_meta": {"pageCount": 2}, "data": [{"id": "1", "judul": "Data Padi"}]}
            )
        raise requests.exceptions.Timeout("timeout page 2")

    extractor.session.get = fake_get

    df = extractor.get_dataset_catalog(target_keywords=["padi"])

    assert len(df) == 1
    assert df.loc[0, "id"] == "1"


def test_fail_fast_true_raises_on_error():
    extractor = APIExtractor("https://example.com/api", fail_fast=True)

    def fake_get(url, timeout):
        raise requests.exceptions.Timeout("network timeout")

    extractor.session.get = fake_get

    with pytest.raises(requests.exceptions.Timeout):
        extractor.get_dataset_catalog(target_keywords=["padi"])
