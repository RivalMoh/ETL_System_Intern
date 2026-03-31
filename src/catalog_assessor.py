import logging
from itertools import combinations
from typing import Dict, List, Any

import pandas as pd
import re
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
        # perlu checking besar kecil untuk menghindari mismatch karena perbedaan kapitalisasi atau spasi
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

    def verify_with_data_sample(
        self, sample_size: int = 5, similarity_threshold: int = 98
    ) -> pd.DataFrame:
        """
        Membandingkan sample data untuk setiap pasangan ID dalam suspect group.

        Menggunakan fuzzy comparison antar baris—bukan exact match—sehingga dataset
        yang hampir identik (typo, variasi minor string) juga terdeteksi sebagai duplikat.

        Args:
            sample_size: Jumlah baris sampel yang diambil per dataset.
            similarity_threshold: Skor minimal (0-100) untuk dianggap duplikat.
                                  100 = identik persis, 90 = hampir identik.
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

            samples: Dict[str, List[str]] = {}  # dataset_id → list of row strings
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

            # Komparasi Pasangan (fuzzy)
            valid_ids = [did for did in ids if did in samples]

            for left_id, right_id in combinations(valid_ids, 2):
                left_rows = samples[left_id]
                right_rows = samples[right_id]

                similarity = self._compute_similarity(left_rows, right_rows)

                if similarity >= similarity_threshold:
                    label = (
                        "Isi data identik (100%)"
                        if similarity == 100.0
                        else f"Isi data hampir identik ({similarity:.1f}%)"
                    )
                    verified_duplicates.append(
                        {
                            "ID_Tabel_A": left_id,
                            "Judul_Tabel_A": id_to_title.get(left_id, ""),
                            "ID_Tabel_B": right_id,
                            "Judul_Tabel_B": id_to_title.get(right_id, ""),
                            "Skor_Kemiripan": round(similarity, 1),
                            "Alasan_Duplikat": label,
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
        text = str(value).strip().lower()
        text = re.sub(r"[^\w\s]", "", text)
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    @staticmethod
    def _build_fingerprint(df_sample: pd.DataFrame) -> List[str]:
        """Kembalikan list of normalized row strings ('col=val|col=val') dari sampel.

        Format ini (bukan hash) memungkinkan fuzzy comparison antar dataset.
        Urutan kolom distabilkan dengan sorted() agar konsisten.
        """
        if "dataset_id" in df_sample.columns:
            df_sample = df_sample.drop(columns=["dataset_id"])

        if df_sample.empty:
            return []

        normalized = (
            df_sample.fillna("")
            .astype(str)
            .apply(lambda col: col.str.strip().str.lower())
        )

        row_strings: List[str] = []
        for _, row in normalized.iterrows():
            row_str = "|".join(
                f"{col}={val}"
                for col, val in sorted(row.items())
                if val != ""
            )
            if row_str:
                row_strings.append(row_str)

        return row_strings

    @staticmethod
    def _compute_similarity(rows_a: List[str], rows_b: List[str]) -> float:
        """Hitung skor kemiripan (0-100) antara dua dataset berdasarkan baris-barisnya.

        Menggunakan bidirectional matching:
        - Setiap baris A dicari pasangan terbaiknya di B (one-way A→B)
        - Setiap baris B dicari pasangan terbaiknya di A (one-way B→A)
        - Skor final = rata-rata kedua arah

        Ini memastikan bahwa skor bersifat simetris dan tidak bias terhadap
        dataset yang lebih pendek.
        """
        if not rows_a or not rows_b:
            return 0.0

        def one_way(src: List[str], tgt: List[str]) -> float:
            total = sum(
                max(fuzz.ratio(r_src, r_tgt) for r_tgt in tgt)
                for r_src in src
            )
            return total / len(src)

        score_ab = one_way(rows_a, rows_b)
        score_ba = one_way(rows_b, rows_a)
        return (score_ab + score_ba) / 2
