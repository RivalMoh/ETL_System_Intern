import logging
from itertools import combinations
from typing import Dict, List, Any

import pandas as pd
from thefuzz import fuzz

from src.extract import APIExtractor

logger = logging.getLogger(__name__)


class CatalogAssessor:
    def __init__(self, df_catalog: pd.DataFrame, extractor: APIExtractor):
        self.df_catalog = df_catalog.copy()
        self.extractor = extractor
        self.suspect_groups: List[Dict[str, Any]] = []
        self.skipped_rows: List[Dict[str, Any]] = []

    def group_by_title_similarity(self, threshold: int = 85):
        """Mengelompokkan ID dataset yang judulnya mirip."""
        self._validate_catalog_columns()
        self._validate_threshold(threshold)

        self.suspect_groups = []
        checked_ids = set()

        rows = list(self.df_catalog[["id", "judul"]].itertuples(index=False, name=None))
        normalized_rows = [(str(row_id), self._normalize_text(title)) for row_id, title in rows]

        for i, (id_a, title_a) in enumerate(normalized_rows):
            if id_a in checked_ids:
                continue

            current_group = [id_a]

            for j in range(i + 1, len(normalized_rows)):
                id_b, title_b = normalized_rows[j]
                if id_b in checked_ids:
                    continue

                sim_score = fuzz.token_set_ratio(title_a, title_b)
                if sim_score >= threshold:
                    current_group.append(id_b)
                    checked_ids.add(id_b)

            checked_ids.add(id_a)

            if len(current_group) > 1:
                self.suspect_groups.append(
                    {
                        "base_title": title_a,
                        "dataset_ids": current_group,
                    }
                )

        logger.info(
            "Ditemukan %s kelompok dataset dengan judul mirip.",
            len(self.suspect_groups),
        )
        return self

    def verify_with_data_sample(self, sample_size: int = 5) -> pd.DataFrame:
        """
        Membandingkan sample data untuk setiap pasangan ID dalam suspect group.
        Hasil duplikat dikembalikan sebagai DataFrame.
        Pasangan/ID yang tidak bisa diverifikasi disimpan di self.skipped_rows.
        """
        verified_duplicates: List[Dict[str, Any]] = []
        self.skipped_rows = []

        for group in self.suspect_groups:
            ids = [str(x) for x in group.get("dataset_ids", [])]
            if len(ids) < 2:
                continue

            samples: Dict[str, str] = {}

            for dataset_id in ids:
                try:
                    df_detail = self.extractor.get_dataset_details(dataset_id)
                except Exception as exc:
                    self.skipped_rows.append(
                        {
                            "dataset_id": dataset_id,
                            "base_title": group.get("base_title", ""),
                            "reason": "fetch_error",
                            "detail": str(exc),
                        }
                    )
                    continue

                if df_detail.empty:
                    self.skipped_rows.append(
                        {
                            "dataset_id": dataset_id,
                            "base_title": group.get("base_title", ""),
                            "reason": "empty_detail",
                            "detail": "No rows returned from API",
                        }
                    )
                    continue

                df_sample = df_detail.head(sample_size).copy()
                fingerprint = self._build_fingerprint(df_sample)
                if not fingerprint:
                    self.skipped_rows.append(
                        {
                            "dataset_id": dataset_id,
                            "base_title": group.get("base_title", ""),
                            "reason": "empty_fingerprint",
                            "detail": "Sample rows could not produce fingerprint",
                        }
                    )
                    continue

                samples[dataset_id] = fingerprint

            # Pairwise comparison within group
            for left_id, right_id in combinations(ids, 2):
                left_fp = samples.get(left_id)
                right_fp = samples.get(right_id)

                if not left_fp or not right_fp:
                    self.skipped_rows.append(
                        {
                            "dataset_id_a": left_id,
                            "dataset_id_b": right_id,
                            "base_title": group.get("base_title", ""),
                            "reason": "missing_sample",
                            "detail": "At least one dataset has no usable sample",
                        }
                    )
                    continue

                if left_fp == right_fp:
                    verified_duplicates.append(
                        {
                            "id_duplikat_a": left_id,
                            "id_duplikat_b": right_id,
                            "alasan": "Isi data identik (sample-based)",
                            "base_title": group.get("base_title", ""),
                        }
                    )

        df_duplicates = pd.DataFrame(verified_duplicates)
        logger.info(
            "Verifikasi selesai, ditemukan %s duplikat berdasarkan isi data.",
            len(df_duplicates),
        )
        logger.info("Total skipped rows/pairs: %s", len(self.skipped_rows))
        return df_duplicates

    def get_skipped_rows(self) -> pd.DataFrame:
        """Mengembalikan catatan skipped rows/pairs sebagai DataFrame."""
        return pd.DataFrame(self.skipped_rows)

    def _validate_catalog_columns(self) -> None:
        required = {"id", "judul"}
        missing = [col for col in required if col not in self.df_catalog.columns]
        if missing:
            raise ValueError(f"Missing required catalog columns: {missing}")

    @staticmethod
    def _validate_threshold(threshold: int) -> None:
        if not isinstance(threshold, int):
            raise TypeError("threshold harus integer")
        if threshold < 0 or threshold > 100:
            raise ValueError("threshold harus dalam rentang 0-100")

    @staticmethod
    def _normalize_text(value: Any) -> str:
        return str(value).strip().lower()

    @staticmethod
    def _build_fingerprint(df_sample: pd.DataFrame) -> str:
        # dataset_id biasanya ditambahkan extractor, buang agar tidak mengganggu matching
        if "dataset_id" in df_sample.columns:
            df_sample = df_sample.drop(columns=["dataset_id"])

        if df_sample.empty:
            return ""

        normalized = (
            df_sample.fillna("")
            .astype(str)
            .apply(lambda col: col.str.strip().str.lower())
        )

        value_list = normalized.values.flatten().tolist()
        cleaned = [v for v in value_list if v != ""]
        if not cleaned:
            return ""

        return "_".join(sorted(cleaned))
