import logging
import pandas as pd
import os
from src.config import AppSettings
from src.loader.client import TargetAPIClient
from src.loader.transform import MigrationTranformer

logger = logging.getLogger(__name__)


class MigrationLoadPipeline:
    def __init__(self, settings: AppSettings):
        self.settings = settings
        self.client = TargetAPIClient(
            base_url=settings.new_base_url, api_key=settings.new_api_key
        )

    def run(self, ready_csv_path: str, mapping_csv_path: str):
        logger.info("Memulai proses load data ke sistem target")

        if not os.path.exists(ready_csv_path) or not os.path.exists(mapping_csv_path):
            logger.error("file load ready atau mapping tidak ditemukan.")
            return

        df_ready = pd.read_csv(ready_csv_path)
        df_mapping = pd.read_csv(mapping_csv_path)

        transformer = MigrationTransformer(df_mapping)
        payloads = transformer.build_payloads(df_ready)

        # eksekusi POST untuk setiap payload
        success_count = 0
        failed_payloads = []

        for item in payloads:
            is_success = self.client.post_data(item["target_id"], item["body"])
            if is_success:
                success_count += 1
            else:
                failed_payloads.append(item)

        logger.info(
            f"Proses load selesai. Total sukses: {success_count}, Total gagal: {len(failed_payloads)}"
        )
        self.client.close()
