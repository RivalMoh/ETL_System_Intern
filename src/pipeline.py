import json
import logging
import pandas as pd
from typing import Set

from src.config import AppSettings
from src.extract import APIExtractor
from src.catalog_assessor import CatalogAssessor
from src.data_assessor import DataAssessor
from src.data_preprocessor import DataPreprocessor
from src.load import LoadGate
from src.reporting import ReportGenerator

logger = logging.getLogger(__name__)


class MigrationPipeline:
    def __init__(self, settings: AppSettings):
        self.settings = settings
        self.extractor = APIExtractor(
            base_url=settings.base_url,
            api_key=settings.api_key,
            max_pages=settings.max_pages,
        )
        self.load_gate = LoadGate(allowed_statuses=settings.allowed_load_statuses)
        self.reporter = ReportGenerator()

    def run(self):
        logger.info("Starting migration pipeline")

        df_catalog = self.extractor.get_dataset_catalog()
        if df_catalog.empty:
            logger.error("No datasets found in catalog. Exiting.")
            self.extractor.close()
            return

        # macro assessment
        catalog_assessor = CatalogAssessor(df_catalog, self.extractor)
        df_duplicates = catalog_assessor.group_by_title_similarity(
            threshold=self.settings.dup_threshold
        ).verify_with_data_sample(sample_size=self.settings.dup_sample_size)
        df_skipped = catalog_assessor.get_skipped_rows()
        duplicate_ids = self._get_duplicate_ids(df_duplicates)

        # micro assessment
        micro_summaries = []
        load_ready_rows = []
        manager_review_rows = []

        for _, row in df_catalog.head(self.settings.max_datasets).iterrows():
            dataset_id = row["id"]
            title = row["judul"]
            is_suspect = str(dataset_id) in duplicate_ids

            # Skip datasets who failed extraction during macro assessment
            try:
                df_detail = self.extractor.get_dataset_details(dataset_id)
            except Exception as e:
                self._record_failure(
                    micro_summaries, dataset_id, title, is_suspect, f"API Error: {e}"
                )
                continue

            if df_detail.empty:
                self._record_failure(
                    micro_summaries,
                    dataset_id,
                    title,
                    is_suspect,
                    "Tabel Kosong (0 baris)",
                )
                continue

            # ── Preprocessing: clean data sebelum assessment ──────────────
            preprocessor = DataPreprocessor(df_detail)
            df_clean = (preprocessor
                        .normalize_columns()
                        .strip_whitespace()
                        .fix_kode_wilayah()
                        .get_result())

            # ── Assessment: flag & warn ───────────────────────────────────
            assessor = DataAssessor(df_clean)
            df_assessed = (assessor
                           .standardize_year_column()
                           .flag_missing_values(self.settings.require_columns)
                           .warn_suspicious_year(
                               min_year=self.settings.year_min,
                               max_year=self.settings.year_max,
                           )
                           .mark_ready())

            # Hitung Statisik
            total_rows = len(df_detail)
            flagged_rows = len(
                df_assessed[
                    df_assessed["migration_status"] == DataAssessor.STATUS_FLAGGED
                ]
            )
            loadable_rows = self.load_gate.build_summary(df_assessed)["loadable_rows"]
            schema_issues = " | ".join(assessor.assessment_issues)

            load_decision = (
                "manager_review_required"
                if (is_suspect or flagged_rows > 0)
                else "ready_for_load"
            )

            micro_summaries.append(
                {
                    "Dataset_Id": dataset_id,
                    "Judul_Tabel": title,
                    "Catalog_Suspect": is_suspect,
                    "Total_Rows": total_rows,
                    "Baris_Bermasalah": flagged_rows,
                    "Baris_Siap_Load": loadable_rows,
                    "Schema_Issues": schema_issues,
                    "Load_Decision": load_decision,
                }
            )

            # json packing
            self._pack_and_route_data(
                df_assessed,
                dataset_id,
                title,
                is_suspect,
                load_decision,
                load_ready_rows,
                manager_review_rows,
            )

        df_micro_summary = pd.DataFrame(micro_summaries)
        df_load_summary = self._build_load_summary(df_micro_summary)

        self.reporter.generate_hybrid_report(
            df_catalog,
            df_duplicates,
            df_skipped,
            df_micro_summary,
            df_load_summary,
            load_ready_rows,
            manager_review_rows,
        )

        self.extractor.close()
        logger.info("Migration pipeline completed")

    # helper functions
    def _pack_and_route_data(
        self,
        df_assessed,
        dataset_id,
        title,
        is_suspect,
        load_decision,
        ready_list,
        review_list,
    ):
        audit_cols = ["migration_status", "flag_reason"]

        if load_decision == "ready_for_load":
            df_ready = self.load_gate.select_rows(df_assessed)
            if not df_ready.empty:
                data_cols = [c for c in df_ready.columns if c not in audit_cols]
                ready_list.append(
                    pd.DataFrame(
                        {
                            "Dataset_Id": dataset_id,
                            "Judul_Tabel": title,
                            "Row_Data_JSON": [
                                json.dumps(r, default=str)
                                for r in df_ready[data_cols].to_dict(orient="records")
                            ],
                        }
                    )
                )
        else:
            # Routing semua baris (flagged MAUPUN ready) ke review_list
            # agar manager bisa melihat konteks lengkap dataset yang suspect/bermasalah.
            # Baris ready dalam dataset suspect TIDAK boleh langsung diload
            # tanpa persetujuan manager, karena validitas katalognya masih dipertanyakan.
            if not df_assessed.empty:
                data_cols = [c for c in df_assessed.columns if c not in audit_cols]
                review_list.append(
                    pd.DataFrame(
                        {
                            "Dataset_Id": dataset_id,
                            "Judul_Tabel": title,
                            "Catalog_Suspect": is_suspect,
                            "migration_status": df_assessed["migration_status"],
                            "flag_reason": df_assessed["flag_reason"],
                            "Row_Data_JSON": [
                                json.dumps(r, default=str)
                                for r in df_assessed[data_cols].to_dict(orient="records")
                            ],
                        }
                    )
                )

    def _record_failure(self, summaries, dataset_id, title, is_suspect, issue):
        summaries.append(
            {
                "Dataset_Id": dataset_id,
                "Judul_Tabel": title,
                "Catalog_Suspect": is_suspect,
                "Total_Rows": 0,
                "Baris_Siap_Load": 0,
                "Baris_Bermasalah": 0,
                "Schema_Issues": issue,
                "Load_Decision": "skipped",
            }
        )

    def _get_duplicate_ids(self, df_duplicates: pd.DataFrame) -> Set[str]:
        if df_duplicates.empty:
            return set()
        duplicates = pd.concat(
            [df_duplicates["ID_Tabel_A"], df_duplicates["ID_Tabel_B"]]
        ).astype("string")
        return set(duplicates.dropna().tolist())

    def _build_load_summary(self, df_micro: pd.DataFrame) -> pd.DataFrame:
        if df_micro.empty:
            return pd.DataFrame()
        return pd.DataFrame(
            [
                {
                    "Total_Dataset_Dinilai": len(df_micro),
                    "Total_Baris_Dinilai": int(df_micro["Total_Rows"].sum()),
                    "Total_Baris_Siap_Load": int(df_micro["Baris_Siap_Load"].sum()),
                    "Total_Baris_Bermasalah": int(df_micro["Baris_Bermasalah"].sum()),
                }
            ]
        )
