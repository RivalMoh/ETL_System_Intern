import logging
import os
import sys
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

from src.catalog_assessor import CatalogAssessor
from src.data_assessor import DataAssessor
from src.extract import APIExtractor

load_dotenv()

logger = logging.getLogger(__name__)


def _setup_logging() -> None:
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("logs/etl-pipeline.log"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def run_assessment_pipeline():
    extractor = APIExtractor(
        base_url=os.getenv("BASE_URL"), api_key=os.getenv("API_Key"), max_pages=1
    )

    date_str = datetime.now().strftime("%Y%m%d_%H%M")
    report_filename = f"data/reports/Audit_migrasi_{date_str}.xlsx"
    os.makedirs("data/reports", exist_ok=True)

    df_catalog = extractor.get_dataset_catalog()

    if df_catalog.empty:
        logger.error("Katalog dataset kosong. Proses assessment dihentikan.")
        return

    # cek duplikasi tabel
    catalog_assessor = CatalogAssessor(df_catalog, extractor)
    df_table_duplicates = catalog_assessor.group_by_title_similarity(
        threshold=85
    ).verify_with_data_sample(sample_size=5)

    if not df_table_duplicates.empty:
        duplicate_ids = pd.concat(
            [df_table_duplicates["id_duplikat_a"], df_table_duplicates["id_duplikat_b"]]
        ).unique().tolist()
        df_valid_catalog = df_catalog[~df_catalog["id"].isin(duplicate_ids)]
    else:
        df_valid_catalog = df_catalog

    # cek row tabel
    micro_assessment_summaries = []

    for _, row in df_valid_catalog.head(5).iterrows():
        dataset_id = row["id"]
        dataset_title = row["judul"]

        df_detail = extractor.get_dataset_details(dataset_id)

        if not df_detail.empty:
            table_assessor = DataAssessor(df_detail)
            df_assessed = table_assessor.flag_missing_values(
                required_columns=["tahun", "jumlah"]
            ).mark_ready()

            total_rows = len(df_detail)
            flagged_rows = len(
                df_assessed[
                    df_assessed["migration_status"] == DataAssessor.STATUS_FLAGGED
                ]
            )

            micro_assessment_summaries.append(
                {
                    "Dataset_Id": dataset_id,
                    "Judul_Tabel": dataset_title,
                    "Total_Rows": total_rows,
                    "Baris Bermasalah": flagged_rows,
                    "Persentase_Bersih": f"{((total_rows - flagged_rows) / total_rows) * 100:.2f}%",
                }
            )

    df_micro_summary = pd.DataFrame(micro_assessment_summaries)

    # export ke excel
    with pd.ExcelWriter(report_filename, engine="openpyxl") as writer:
        # sheet 1 : ringkasan duplikasi tabel
        df_table_duplicates.to_excel(writer, sheet_name="Tabel Duplikat", index=False)

        # sheet 2 : ringkasan kualitas data
        df_micro_summary.to_excel(writer, sheet_name="Kualitas Data", index=False)

        # sheet 3 : katalog original
        df_catalog.to_excel(writer, sheet_name="Katalog Original", index=False)
    logger.info("Laporan assessment berhasil disimpan di %s", report_filename)


if __name__ == "__main__":
    _setup_logging()
    run_assessment_pipeline()

