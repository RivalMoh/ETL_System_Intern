import os
import logging
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)


class ReportGenerator:
    def __init__(self, output_dir: str = "data/reports"):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

        date_str = datetime.now().strftime("%Y%m%d_%H%M")
        self.report_excel = f"{self.output_dir}/Audit_Migrasi_{date_str}.xlsx"
        self.ready_csv = f"{self.output_dir}/Load_Ready_{date_str}.csv"
        self.review_csv = f"{self.output_dir}/Manager_review_{date_str}.csv"

    def generate_hybrid_report(
        self,
        df_catalog,
        df_duplicates,
        df_skipped,
        df_micro_summary,
        df_load_summary,
        load_ready_rows: list,
        manager_review_rows: list,
    ):
        logger.info(f"Generating report Excel & CSV files in {self.output_dir}")

        df_ready = (
            pd.concat(load_ready_rows, ignore_index=True)
            if load_ready_rows
            else pd.DataFrame()
        )
        df_review = (
            pd.concat(manager_review_rows, ignore_index=True)
            if manager_review_rows
            else pd.DataFrame()
        )

        if not df_ready.empty:
            df_ready.to_csv(self.ready_csv, index=False)
            logger.info(f"Saved Load Ready report to {self.ready_csv}")

        if not df_review.empty:
            df_review.to_csv(self.review_csv, index=False)
            logger.info(f"Saved Manager Review report to {self.review_csv}")

        with pd.ExcelWriter(self.report_excel, engine="openpyxl") as writer:
            df_duplicates.to_excel(writer, sheet_name="Tabel Duplikat", index=False)
            df_skipped.to_excel(writer, sheet_name="Tabel Skip", index=False)
            df_micro_summary.to_excel(writer, sheet_name="Kualitas Data", index=False)
            df_load_summary.to_excel(writer, sheet_name="Summary", index=False)
            df_catalog.to_excel(writer, sheet_name="Catalog", index=False)

        logger.info(f"Saved full audit report to {self.report_excel}")
