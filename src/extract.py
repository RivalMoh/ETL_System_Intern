import logging
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class APIExtractor:
    def __init__(
        self,
        base_url: str,
        api_key: Optional[str] = None,
        timeout_catalog: int = 10,
        timeout_detail: int = 15,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
        fail_fast: bool = True,
        max_pages: Optional[int] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout_catalog = timeout_catalog
        self.timeout_detail = timeout_detail
        self.fail_fast = fail_fast
        self.max_pages = max_pages

        self.session = requests.Session()
        headers = {"Accept": "application/json"}
        if api_key:
            clean_key = api_key.strip()

            if not clean_key.lower().startswith(
                "bearer "
            ) and not clean_key.lower().startswith("token "):
                headers["Authorization"] = f"Bearer {clean_key}"
            else:
                headers["Authorization"] = api_key

        self.session.headers.update(headers)

        retry = Retry(
            total=max_retries,
            connect=max_retries,
            read=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def close(self) -> None:
        self.session.close()

    def __enter__(self) -> "APIExtractor":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def get_dataset_catalog(
        self, target_keywords: Optional[Sequence[str]] = None
    ) -> pd.DataFrame:
        """
        Ambil katalog dataset dari endpoint utama (paginated).
        target_keywords: daftar kata kunci untuk filter judul.
        """
        keywords = self._normalize_keywords(target_keywords)
        all_datasets: List[Dict[str, Any]] = []
        page = 1
        total_pages = 1

        while page <= total_pages:
            if self.max_pages is not None and page > self.max_pages:
                logger.warning(
                    "Reached max_pages=%s; stop catalog extraction.", self.max_pages
                )
                break

            url = f"{self.base_url}?page={page}"
            try:
                data = self._get_json(url, timeout=self.timeout_catalog)

                if page == 1:
                    total_pages = self._safe_page_count(data)
                    logger.info("Ditemukan %s halaman dataset.", total_pages)

                for item in data.get("data", []):
                    if not isinstance(item, dict):
                        continue

                    judul = str(item.get("judul", "")).lower()
                    if not keywords or any(kw in judul for kw in keywords):
                        all_datasets.append(item)

                logger.debug(
                    "Katalog halaman %s/%s berhasil diambil.", page, total_pages
                )
                page += 1

            except (requests.exceptions.RequestException, ValueError, TypeError) as exc:
                logger.error("Gagal mengambil katalog pada halaman %s: %s", page, exc)
                if self.fail_fast:
                    raise
                break

        logger.info("Total dataset yang diambil: %s", len(all_datasets))
        return pd.DataFrame(all_datasets)

    def get_dataset_details(self, dataset_id: str) -> pd.DataFrame:
        """
        Ambil seluruh record detail untuk satu dataset_id (paginated).
        """
        logger.info("Mengekstrak detail untuk dataset ID: %s", dataset_id)

        all_records: List[Dict[str, Any]] = []
        page = 1
        total_pages = 1

        while page <= total_pages:
            if self.max_pages is not None and page > self.max_pages:
                logger.warning(
                    "Reached max_pages=%s for dataset_id=%s; stop detail extraction.",
                    self.max_pages,
                    dataset_id,
                )
                break

            url = f"{self.base_url}/{dataset_id}?page={page}"
            try:
                data = self._get_json(url, timeout=self.timeout_detail)

                if page == 1:
                    total_pages = self._safe_page_count(data)

                records = data.get("data", [])
                if isinstance(records, list):
                    all_records.extend(records)

                logger.debug(
                    "ID %s | Halaman %s/%s berhasil diambil.",
                    dataset_id[:8] + "...",
                    page,
                    total_pages,
                )
                page += 1

            except (requests.exceptions.RequestException, ValueError, TypeError) as exc:
                logger.error(
                    "Gagal mengambil detail untuk dataset ID %s pada halaman %s: %s",
                    dataset_id,
                    page,
                    exc,
                )
                if self.fail_fast:
                    raise
                break

        df = pd.DataFrame(all_records)
        # if not df.empty:
        #     df["dataset_id"] = dataset_id

        logger.info(
            "Berhasil menarik %s record untuk dataset ID %s",
            len(all_records),
            dataset_id,
        )
        return df

    def _get_json(self, url: str, timeout: int) -> Dict[str, Any]:
        response = self.session.get(url, timeout=timeout)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _normalize_keywords(
        target_keywords: Optional[Sequence[str]],
    ) -> List[str]:
        if not target_keywords:
            return []
        return [
            str(keyword).lower().strip()
            for keyword in target_keywords
            if str(keyword).strip()
        ]

    @staticmethod
    def _safe_page_count(payload: Dict[str, Any]) -> int:
        page_count = payload.get("_meta", {}).get("pageCount", 1)
        try:
            page_count_int = int(page_count)
            return page_count_int if page_count_int > 0 else 1
        except (TypeError, ValueError):
            logger.warning("Invalid pageCount value: %s. Defaulting to 1.", page_count)
            return 1
