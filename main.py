import logging
import sys
import os
import argparse
from src.config import AppSettings
from src.pipeline import MigrationPipeline
from src.loader.pipeline import MigrationLoadPipeline


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
    settings = AppSettings()  # Muat Pengaturan dari .env

    parser = argparse.ArgumentParser(
        description="ETL Pipeline untuk Migrasi Data Satu Data Jateng"
    )
    parser.add_argument(
        "--mode",
        choices=["audit", "migrate"],
        required=True,
        help="Pilih mode operasi: 'audit' untuk penilaian data, 'migrate' untuk proses migrasi",
    )

    # argumen tambahan untuk mode migrate
    parser.add_argument(
        "--ready_file", 
        type=str, 
        default="data/load_ready.csv", 
        help="Path ke file CSV yang berisi data siap load"
    )

    args = parser.parse_args()

    if args.mode == "audit":
        if not settings.base_url:
            logging.error("BASE_URL belum diset di .env. Membatalkan eksekusi.")
            sys.exit(1)

        # 2. Inisialisasi dan Jalankan Pipeline
        pipeline = MigrationPipeline(settings)
        pipeline.run()

    elif args.mode == "migrate":
        if not settings.new_base_url or not settings.new_api_key:
            logging.error(
                "NEW_BASE_URL atau NEW_API_KEY belum diset di .env. Membatalkan eksekusi."
            )
            sys.exit(1)

        load_pipeline = MigrationLoadPipeline(settings)
        load_pipeline.run(args.ready_file)
