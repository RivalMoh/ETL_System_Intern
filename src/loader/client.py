import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


class TargetAPIClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url
        self.session = requests.Session()

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        if api_key:
            headers["Authorization"] = (
                api_key
                if api_key.lower().startswith("bearer ")
                else f"Bearer {api_key}"
            )

        self.session.headers.update(headers)

        # setup retries for idempotent requests
        retry = Retry(
            total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def get_catalog(self) -> List[Dict[str, Any]]:
        """Fetches the list of Home/Catalogs from the target API."""
        logger.info(f"Mengambil Katalog dari sistem target: {self.base_url}")
        try:
            response = self.session.get(self.base_url, timeout=15)
            response.raise_for_status()
            data = response.json()
            return data.get("data", [])
        except requests.exceptions.RequestException as e:
            logger.error(f"Gagal mengambil katalog: {e}")
            return []

    def post_data(self, target_id: int, payload: Dict[str, Any]) -> bool:
        """Mengirim data ke sistem baru berdasarkan id_target dan payload yang sudah diformat"""
        url = f"{self.base_url}/{target_id}"
        response = None
        try:
            response = self.session.post(url, json=payload, timeout=20)
            response.raise_for_status()
            logger.info(
                f"Sukses POST ke ID {target_id} | Tahun: {payload.get('tahun_data')}"
            )
            return True
        except requests.exceptions.RequestException as e:
            error_msg = response.text if response else str(e)
            logger.error(f"Gagal POST ke ID {target_id} | Error: {error_msg}")
            return False

    def close(self):
        """Menutup sesi HTTP."""
        self.session.close()
