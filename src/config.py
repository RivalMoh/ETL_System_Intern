import os
import logging
from typing import List, Optional, Sequence
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


class AppSettings:
    def __init__(self):
        self.base_url = os.getenv("BASE_URL")
        self.api_key = os.getenv("API_KEY")
        self.max_pages = self._read_int("MAX_PAGES", default=1, min_value=1)
        self.max_datasets = self._read_int(
            "MAX_DATASETS_TO_ASSESS", default=5, min_value=1
        )
        self.dup_threshold = self._read_int(
            "DUPLICATE_TITLE_THRESHOLD", default=85, min_value=0, max_value=100
        )
        self.dup_sample_size = self._read_int(
            "DUPLICATE_SAMPLE_SIZE", default=5, min_value=1
        )

        self.require_columns = self._read_list("REQUIRED_COLUMNS", ["tahun", "jumlah"])
        self.allowed_load_statuses = self._read_list("LOAD_ALLOWED_STATUSES", ["ready"])

        self.new_base_url = os.getenv("NEW_BASE_URL").rstrip("/")
        self.new_api_key = os.getenv("NEW_API_KEY")

    @staticmethod
    def _read_int(
        name: str,
        default: int,
        min_value: Optional[int] = None,
        max_value: Optional[int] = None,
    ) -> int:
        raw_value = os.getenv(name)
        if not raw_value:
            logger.warning(f"{name} not set, using default {default}")
            return default
        try:
            parsed = int(raw_value)
            if min_value is not None and parsed < min_value:
                return default
            if max_value is not None and parsed > max_value:
                return default
            return parsed
        except ValueError:
            logger.warning(f"{name} not set, using default {default}")
            return default

    @staticmethod
    def _read_list(name: str, default: Sequence[str]) -> List[str]:
        env_value = os.getenv(name)
        if not env_value:
            logger.warning(f"{name} not set, using default {default}")
            return list(default)

        parsed = [val.strip().lower() for val in env_value.split(",") if val.strip()]
        return parsed if parsed else list(default)
