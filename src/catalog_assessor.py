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
        normalized_rows = [
            (str(row_id), self._normalize_text(title)) for row_id, title in rows
        ]

        for i, (id_a, title_a) in enumerate(normalized_rows):
            if id_a in checked_ids:
                continue

            current_group = [id_a]

            for j in range(i + 1, len(normalized_rows)):
                id_b, title_b = normalized_rows[j]
                if id_b in checked_ids:
                    continue

                sim_score = fuzz.token_sort_ratio(title_a, title_b)
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

        # help map id to title
        id_to_title = dict(
            zip(self.df_catalog["id"].astype(str), self.df_catalog["judul"])
        )

        for group in self.suspect_groups:
            ids = [str(x) for x in group.get("dataset_ids", [])]
            if len(ids) < 2:
                continue

            samples: Dict[str, str] = {}
            group_base_title = group.get("base_title", "")

            # TAHAP 1: Ekstraksi Sampel per ID
            for dataset_id in ids:
                # Ambil judul asli dari dataset ini
                actual_title = id_to_title.get(dataset_id, "Judul Tidak Diketahui")

                try:
                    df_detail = self.extractor.get_dataset_details(dataset_id)
                except Exception as exc:
                    # Laporan Skipped yang jauh lebih jelas
                    self.skipped_rows.append(
                        {
                            "Dataset_ID_Gagal": dataset_id,
                            "Judul_Asli_Dataset": actual_title,
                            "Kategori_Error": "Gagal Tarik API (Fetch Error)",
                            "Pesan_Detail": str(exc),
                            "Grup_Pencarian_Awal": group_base_title,
                        }
                    )
                    continue

                if df_detail.empty:
                    self.skipped_rows.append(
                        {
                            "Dataset_ID_Gagal": dataset_id,
                            "Judul_Asli_Dataset": actual_title,
                            "Kategori_Error": "Tabel Kosong (Empty Detail)",
                            "Pesan_Detail": "API sukses, tetapi tidak ada baris data di dalamnya",
                            "Grup_Pencarian_Awal": group_base_title,
                        }
                    )
                    continue

                df_sample = df_detail.head(sample_size).copy()
                fingerprint = self._build_fingerprint(df_sample)

                if not fingerprint:
                    self.skipped_rows.append(
                        {
                            "Dataset_ID_Gagal": dataset_id,
                            "Judul_Asli_Dataset": actual_title,
                            "Kategori_Error": "Data Tidak Valid (Empty Fingerprint)",
                            "Pesan_Detail": "Baris sampel hanya berisi nilai kosong/NaN",
                            "Grup_Pencarian_Awal": group_base_title,
                        }
                    )
                    continue

                samples[dataset_id] = fingerprint

            # TAHAP 2: Komparasi Pasangan (HANYA untuk ID yang berhasil ditarik)
            valid_ids = [did for did in ids if did in samples]

            for left_id, right_id in combinations(valid_ids, 2):
                left_fp = samples[left_id]
                right_fp = samples[right_id]

                if left_fp == right_fp:
                    # Laporan Duplikat yang super informatif
                    verified_duplicates.append(
                        {
                            "ID_Tabel_A": left_id,
                            "Judul_Tabel_A": id_to_title.get(left_id, ""),
                            "ID_Tabel_B": right_id,
                            "Judul_Tabel_B": id_to_title.get(right_id, ""),
                            "Alasan_Duplikat": "Isi data identik (Berdasarkan Sampel 5 Baris)",
                            "Grup_Pencarian_Awal": group_base_title,
                        }
                    )

        df_duplicates = pd.DataFrame(verified_duplicates)
        logger.info(
            "Verifikasi selesai, ditemukan %s duplikat berdasarkan isi data.",
            len(df_duplicates),
        )
        logger.info("Total tabel yang di-skip (Error): %s", len(self.skipped_rows))
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
