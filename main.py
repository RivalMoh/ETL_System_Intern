import logging
import sys
import os
from src.config import AppSettings
from src.pipeline import MigrationPipeline


def setup_logging():
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("logs/etl-pipeline.log"),
            logging.StreamHandler(sys.stdout),
        ],
    )


if __name__ == "__main__":
    setup_logging()

    # 1. Muat Pengaturan dari .env
    settings = AppSettings()

    if not settings.base_url:
        logging.error("BASE_URL belum diset di .env. Membatalkan eksekusi.")
        sys.exit(1)

    # 2. Inisialisasi dan Jalankan Pipeline
    pipeline = MigrationPipeline(settings)
    pipeline.run()
