"""
Unit tests untuk TargetAPIClient (src/loader/client.py).

Semua HTTP calls di-mock menggunakan unittest.mock.patch agar tidak
membutuhkan koneksi ke API nyata.
"""
import pytest
import requests
from unittest.mock import MagicMock, patch

from src.loader.client import TargetAPIClient


# ─── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def client():
    """Fresh TargetAPIClient dengan URL dan API key dummy."""
    return TargetAPIClient(base_url="https://api.target.test/v1/data", api_key="secret-token")


def make_mock_response(payload: dict, status_code: int = 200) -> MagicMock:
    """Helper: buat mock response yang meniru requests.Response."""
    mock = MagicMock()
    mock.status_code = status_code
    mock.json.return_value = payload
    mock.text = str(payload)
    if status_code >= 400:
        mock.raise_for_status.side_effect = requests.exceptions.HTTPError(
            f"HTTP {status_code}"
        )
    else:
        mock.raise_for_status.return_value = None
    return mock


# ─── Authorization Header ────────────────────────────────────────────────────


def test_bare_api_key_gets_bearer_prefix():
    """Key tanpa prefix 'Bearer' harus otomatis ditambahkan."""
    c = TargetAPIClient("https://api.test", api_key="my-raw-token")
    assert c.session.headers["Authorization"] == "Bearer my-raw-token"


def test_api_key_already_bearer_not_doubled():
    """Key yang sudah dimulai 'bearer ' tidak boleh digandakan prefixnya."""
    c = TargetAPIClient("https://api.test", api_key="bearer already-prefixed")
    assert c.session.headers["Authorization"] == "bearer already-prefixed"


def test_empty_api_key_no_auth_header():
    """Jika api_key kosong/None, header Authorization tidak ditambahkan."""
    c = TargetAPIClient("https://api.test", api_key="")
    assert "Authorization" not in c.session.headers


# ─── get_catalog() ───────────────────────────────────────────────────────────


def test_get_catalog_returns_data_list(client):
    """Sukses: kembalikan list item dari key 'data'."""
    catalog_items = [
        {"id": 1, "judul": "Data Padi"},
        {"id": 2, "judul": "Data Jagung"},
    ]
    mock_resp = make_mock_response({"data": catalog_items})

    with patch.object(client.session, "get", return_value=mock_resp):
        result = client.get_catalog()

    assert result == catalog_items
    assert len(result) == 2


def test_get_catalog_missing_data_key_returns_empty(client):
    """Jika response tidak punya key 'data', kembalikan list kosong."""
    mock_resp = make_mock_response({"meta": {"total": 0}})

    with patch.object(client.session, "get", return_value=mock_resp):
        result = client.get_catalog()

    assert result == []


def test_get_catalog_http_error_returns_empty(client):
    """Jika API return 500, kembalikan list kosong tanpa raise exception."""
    mock_resp = make_mock_response({}, status_code=500)

    with patch.object(client.session, "get", return_value=mock_resp):
        result = client.get_catalog()

    assert result == []


def test_get_catalog_network_error_returns_empty(client):
    """Jika ada ConnectionError (network down), kembalikan list kosong."""
    with patch.object(
        client.session, "get", side_effect=requests.exceptions.ConnectionError("timeout")
    ):
        result = client.get_catalog()

    assert result == []


# ─── post_data() ─────────────────────────────────────────────────────────────


def test_post_data_success_returns_true(client):
    """POST berhasil (HTTP 200) → return True."""
    mock_resp = make_mock_response({"message": "ok"}, status_code=200)
    payload = {"tahun_data": 2023, "data": [{"jumlah": 100}]}

    with patch.object(client.session, "post", return_value=mock_resp):
        result = client.post_data(target_id=42, payload=payload)

    assert result is True


def test_post_data_posts_to_correct_url(client):
    """URL POST harus berformat {base_url}/{target_id}."""
    mock_resp = make_mock_response({}, status_code=200)
    payload = {"tahun_data": 2024, "data": []}

    with patch.object(client.session, "post", return_value=mock_resp) as mock_post:
        client.post_data(target_id=99, payload=payload)

    called_url = mock_post.call_args[0][0]
    assert called_url == "https://api.target.test/v1/data/99"


def test_post_data_sends_json_payload(client):
    """Payload harus dikirim sebagai JSON body, bukan form data."""
    mock_resp = make_mock_response({}, status_code=200)
    payload = {"tahun_data": 2023, "data": [{"kab": "Semarang", "nilai": 500}]}

    with patch.object(client.session, "post", return_value=mock_resp) as mock_post:
        client.post_data(target_id=1, payload=payload)

    assert mock_post.call_args[1]["json"] == payload


def test_post_data_http_error_returns_false(client):
    """POST yang mendapat HTTP 422 → return False tanpa raise."""
    mock_resp = make_mock_response({"error": "Unprocessable"}, status_code=422)

    with patch.object(client.session, "post", return_value=mock_resp):
        result = client.post_data(target_id=5, payload={"tahun_data": 2020, "data": []})

    assert result is False


def test_post_data_network_error_returns_false(client):
    """POST yang gagal karena ConnectionError → return False tanpa raise."""
    with patch.object(
        client.session, "post", side_effect=requests.exceptions.ConnectionError("unreachable")
    ):
        result = client.post_data(target_id=5, payload={"tahun_data": 2021, "data": []})

    assert result is False


def test_post_data_network_error_response_is_none(client):
    """Saat network error (response=None), tidak boleh raise UnboundLocalError."""
    with patch.object(
        client.session, "post", side_effect=requests.exceptions.Timeout("timed out")
    ):
        # Bug kritis: jika response tidak di-inisialisasi None, akan UnboundLocalError
        result = client.post_data(target_id=7, payload={"tahun_data": 2022, "data": []})

    assert result is False  # tidak boleh raise exception apapun


# ─── close() ─────────────────────────────────────────────────────────────────


def test_close_terminates_session(client):
    """close() harus menutup HTTP session."""
    with patch.object(client.session, "close") as mock_close:
        client.close()
    mock_close.assert_called_once()
