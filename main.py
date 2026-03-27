import logging
import os
import sys
from datetime import datetime
from typing import List, Optional, Sequence, Set

import pandas as pd
from dotenv import load_dotenv

from src.catalog_assessor import CatalogAssessor
from src.data_assessor import DataAssessor
from src.extract import APIExtractor
from src.load import LoadGate

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


def _read_int_env(
    name: str,
    default: int,
    min_value: Optional[int] = None,
    max_value: Optional[int] = None,
) -> int:
    raw_value = os.getenv(name)
    if raw_value is None or not raw_value.strip():
        return default

    try:
        parsed = int(raw_value)
    except ValueError:
        logger.warning(
            "ENV %s=%s bukan integer. Gunakan default=%s", name, raw_value, default
        )
        return default

    if min_value is not None and parsed < min_value:
        logger.warning(
            "ENV %s=%s di bawah minimum %s. Gunakan default=%s",
            name,
            parsed,
            min_value,
            default,
        )
        return default

    if max_value is not None and parsed > max_value:
        logger.warning(
            "ENV %s=%s di atas maksimum %s. Gunakan default=%s",
            name,
            parsed,
            max_value,
            default,
        )
        return default

    return parsed


def _read_required_columns(default_columns: Sequence[str]) -> List[str]:
    env_value = os.getenv("REQUIRED_COLUMNS")
    if not env_value:
        return list(default_columns)

    parsed = [column.strip() for column in env_value.split(",") if column.strip()]
    return parsed if parsed else list(default_columns)


def _read_allowed_load_statuses(default_statuses: Sequence[str]) -> List[str]:
    env_value = os.getenv("LOAD_ALLOWED_STATUSES")
    if not env_value:
        return list(default_statuses)

    parsed = [
        status.strip().lower() for status in env_value.split(",") if status.strip()
    ]
    return parsed if parsed else list(default_statuses)


def _extract_duplicate_ids(df_duplicates: pd.DataFrame) -> Set[str]:
    if df_duplicates.empty:
        return set()

    duplicates = pd.concat(
        [df_duplicates["id_duplikat_a"], df_duplicates["id_duplikat_b"]]
    ).astype("string")
    return set(duplicates.dropna().tolist())


def _build_clean_ratio(total_rows: int, flagged_rows: int) -> str:
    if total_rows == 0:
        return "0.00%"
    return f"{((total_rows - flagged_rows) / total_rows) * 100:.2f}%"


def _build_load_ratio(total_rows: int, loadable_rows: int) -> str:
    if total_rows == 0:
        return "0.00%"
    return f"{(loadable_rows / total_rows) * 100:.2f}%"


def run_assessment_pipeline() -> None:
    base_url = os.getenv("BASE_URL")
    if not base_url:
        logger.error("BASE_URL belum diset. Proses assessment dihentikan.")
        return

    api_key = os.getenv("API_KEY") or os.getenv("API_Key")
    max_pages = _read_int_env("MAX_PAGES", default=1, min_value=1)
    max_datasets_to_assess = _read_int_env(
        "MAX_DATASETS_TO_ASSESS", default=5, min_value=1
    )
    duplicate_threshold = _read_int_env(
        "DUPLICATE_TITLE_THRESHOLD", default=85, min_value=0, max_value=100
    )
    duplicate_sample_size = _read_int_env(
        "DUPLICATE_SAMPLE_SIZE", default=5, min_value=1
    )
    required_columns = _read_required_columns(default_columns=["tahun", "jumlah"])
    allowed_load_statuses = _read_allowed_load_statuses(default_statuses=["ready"])

    extractor = APIExtractor(base_url=base_url, api_key=api_key, max_pages=max_pages)
    load_gate = LoadGate(allowed_statuses=allowed_load_statuses)

    date_str = datetime.now().strftime("%Y%m%d_%H%M")
    report_filename = f"data/reports/Audit_migrasi_{date_str}.xlsx"
    os.makedirs("data/reports", exist_ok=True)

    df_catalog = extractor.get_dataset_catalog()

    if df_catalog.empty:
        logger.error("Katalog dataset kosong. Proses assessment dihentikan.")
        extractor.close()
        return

    # cek duplikasi tabel (soft-tag: tidak di-drop)
    catalog_assessor = CatalogAssessor(df_catalog, extractor)
    df_table_duplicates = catalog_assessor.group_by_title_similarity(
        threshold=duplicate_threshold
    ).verify_with_data_sample(sample_size=duplicate_sample_size)
    df_duplicate_skipped = catalog_assessor.get_skipped_rows()
    duplicate_ids = _extract_duplicate_ids(df_table_duplicates)

    # cek row tabel
    micro_assessment_summaries = []
    load_ready_rows = []
    manager_review_rows = []

    for _, row in df_catalog.head(max_datasets_to_assess).iterrows():
        dataset_id = row["id"]
        dataset_title = row["judul"]
        is_catalog_suspect = str(dataset_id) in duplicate_ids

        df_detail = extractor.get_dataset_details(dataset_id)

        if df_detail.empty:
            micro_assessment_summaries.append(
                {
                    "Dataset_Id": dataset_id,
                    "Judul_Tabel": dataset_title,
                    "Catalog_Suspect": is_catalog_suspect,
                    "Total_Rows": 0,
                    "Baris Bermasalah": 0,
                    "Baris Siap Load": 0,
                    "Persentase_Bersih": "0.00%",
                    "Persentase_Siap_Load": "0.00%",
                    "Schema_Issues": "",
                    "Load_Decision": "manager_review_empty_detail",
                }
            )
            continue

        table_assessor = DataAssessor(df_detail)
        df_assessed = table_assessor.flag_missing_values(
            required_columns=required_columns
        ).mark_ready()

        total_rows = len(df_assessed)
        flagged_rows = len(
            df_assessed[df_assessed["migration_status"] == DataAssessor.STATUS_FLAGGED]
        )
        load_summary = load_gate.build_summary(df_assessed)
        loadable_rows = load_summary["loadable_rows"]
        schema_issues = " | ".join(table_assessor.assessment_issues)

        if is_catalog_suspect or flagged_rows > 0 or schema_issues:
            load_decision = "manager_review_required"
        else:
            load_decision = "ready_for_load"

        micro_assessment_summaries.append(
            {
                "Dataset_Id": dataset_id,
                "Judul_Tabel": dataset_title,
                "Catalog_Suspect": is_catalog_suspect,
                "Total_Rows": total_rows,
                "Baris Bermasalah": flagged_rows,
                "Baris Siap Load": loadable_rows,
                "Persentase_Bersih": _build_clean_ratio(total_rows, flagged_rows),
                "Persentase_Siap_Load": _build_load_ratio(total_rows, loadable_rows),
                "Schema_Issues": schema_issues,
                "Load_Decision": load_decision,
            }
        )

        if load_decision == "ready_for_load":
            df_ready = load_gate.select_rows(df_assessed)
            if not df_ready.empty:
                df_ready = df_ready.copy()
                df_ready["dataset_title"] = dataset_title
                load_ready_rows.append(df_ready)
        else:
            df_review = df_assessed.copy()
            df_review["dataset_title"] = dataset_title
            df_review["catalog_suspect"] = is_catalog_suspect
            df_review["load_decision"] = load_decision
            manager_review_rows.append(df_review)

    df_micro_summary = pd.DataFrame(micro_assessment_summaries)
    df_load_ready = (
        pd.concat(load_ready_rows, ignore_index=True)
        if load_ready_rows
        else pd.DataFrame()
    )
    df_manager_review = (
        pd.concat(manager_review_rows, ignore_index=True)
        if manager_review_rows
        else pd.DataFrame()
    )

    # ringkasan load gate lintas semua dataset yang diproses
    total_assessed_rows = (
        int(df_micro_summary["Total_Rows"].sum()) if not df_micro_summary.empty else 0
    )
    total_loadable_rows = (
        int(df_micro_summary["Baris Siap Load"].sum())
        if not df_micro_summary.empty
        else 0
    )
    total_flagged_rows = (
        int(df_micro_summary["Baris Bermasalah"].sum())
        if not df_micro_summary.empty
        else 0
    )
    total_catalog_suspect = (
        int(df_micro_summary["Catalog_Suspect"].sum())
        if not df_micro_summary.empty
        else 0
    )
    df_load_summary = pd.DataFrame(
        [
            {
                "Dataset_Dinilai": len(df_micro_summary),
                "Total_Baris_Dinilai": total_assessed_rows,
                "Total_Baris_Siap_Load": total_loadable_rows,
                "Total_Baris_Bermasalah": total_flagged_rows,
                "Total_Dataset_Suspect_Katalog": total_catalog_suspect,
            }
        ]
    )

    # export ke excel
    with pd.ExcelWriter(report_filename, engine="openpyxl") as writer:
        # sheet 1 : ringkasan duplikasi tabel
        df_table_duplicates.to_excel(writer, sheet_name="Tabel Duplikat", index=False)

        # sheet 2 : pasangan/row yang tidak bisa diverifikasi saat cek duplikasi
        df_duplicate_skipped.to_excel(
            writer, sheet_name="Duplikat Skipped", index=False
        )

        # sheet 3 : ringkasan kualitas data
        df_micro_summary.to_excel(writer, sheet_name="Kualitas Data", index=False)

        # sheet 4 : ringkasan load gate
        df_load_summary.to_excel(writer, sheet_name="Load Summary", index=False)

        # sheet 5 : detail baris yang siap load (hanya dataset ready_for_load)
        df_load_ready.to_excel(writer, sheet_name="Load Ready Rows", index=False)

        # sheet 6 : detail baris yang harus ditinjau manajerial
        df_manager_review.to_excel(
            writer, sheet_name="Manager Review Rows", index=False
        )

        # sheet 7 : katalog original
        df_catalog.to_excel(writer, sheet_name="Katalog Original", index=False)

    logger.info("Laporan assessment berhasil disimpan di %s", report_filename)
    logger.info(
        "Assessment selesai | dataset=%s | total_rows=%s | ready_rows=%s | flagged_rows=%s",
        len(df_micro_summary),
        total_assessed_rows,
        total_loadable_rows,
        total_flagged_rows,
    )
    extractor.close()


if __name__ == "__main__":
    _setup_logging()
    run_assessment_pipeline()
