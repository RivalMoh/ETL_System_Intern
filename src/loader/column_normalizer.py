"""
Column Normalizer — Menormalisasi nama kolom dari sistem legacy
ke nama standar yang diharapkan oleh API target.

Strategi pencocokan (berurutan):
1. Exact match     → kolom sudah sesuai standar, skip
2. Explicit alias  → dicari di column_mapping.json "column_aliases"
3. Case normalize  → lowercase match (Kecamatan → kecamatan)
4. Fuzzy match     → menggunakan thefuzz (jml → jumlah)
5. Unmapped        → log warning, kolom tetap dikirim apa adanya
"""
import json
import logging
import os
from typing import Any, Dict, List, Optional, Set

from thefuzz import fuzz

logger = logging.getLogger(__name__)

# Default path untuk file konfigurasi
_DEFAULT_MAPPING_FILE = "data/column_mapping.json"


class ColumnNormalizer:
    """
    Menormalisasi nama kolom data sebelum dikirim ke API target.

    Mendukung dua sumber mapping:
    - Explicit alias dari column_mapping.json (prioritas utama)
    - Fuzzy matching otomatis sebagai fallback
    """

    def __init__(
        self,
        mapping_file: str = _DEFAULT_MAPPING_FILE,
        fuzzy_threshold: int = 80,
        target_columns: Optional[List[str]] = None,
    ):
        self.fuzzy_threshold = fuzzy_threshold
        self.target_columns = target_columns or []

        # Load explicit aliases dari file JSON
        self.explicit_aliases: Dict[str, str] = {}
        self._load_mapping_file(mapping_file)

        # Cache resolved mappings per dataset (agar tidak recompute)
        self._resolved_cache: Dict[str, Dict[str, str]] = {}

        # Kumpulkan semua rename yang dilakukan (untuk report)
        self._rename_log: List[Dict[str, str]] = []

    def normalize_record(
        self, record: Dict[str, Any], dataset_id: str = ""
    ) -> Dict[str, Any]:
        """
        Rename semua key dalam record dict sesuai mapping yang ditemukan.

        Returns:
            Dict baru dengan key yang sudah dinormalisasi.
        """
        col_mapping = self._get_column_mapping(record.keys(), dataset_id)
        if not col_mapping:
            return record

        normalized = {}
        for key, value in record.items():
            new_key = col_mapping.get(key, key)
            normalized[new_key] = value
        return normalized

    def get_rename_report(self) -> List[Dict[str, str]]:
        """Kembalikan log semua rename yang dilakukan selama proses."""
        return self._rename_log

    def save_rename_report(self, output_path: str = "data/reports/column_mapping_report.csv") -> None:
        """Simpan rekap rename ke CSV."""
        if not self._rename_log:
            logger.info("Tidak ada kolom yang di-rename. Report tidak dibuat.")
            return

        import pandas as pd

        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df = pd.DataFrame(self._rename_log)
        df.to_csv(output_path, index=False)
        logger.info(f"Column mapping report disimpan di: {output_path}")

    # ─── Private ─────────────────────────────────────────────────────────────

    def _load_mapping_file(self, mapping_file: str) -> None:
        """Load explicit column aliases dari file JSON."""
        if not os.path.exists(mapping_file):
            logger.warning(
                f"File column mapping tidak ditemukan: {mapping_file}. "
                f"Hanya fuzzy matching yang akan digunakan."
            )
            return

        try:
            with open(mapping_file, "r", encoding="utf-8") as f:
                config = json.load(f)

            self.explicit_aliases = config.get("column_aliases", {})
            # Normalisasi key alias ke lowercase
            self.explicit_aliases = {
                k.strip().lower(): v.strip().lower()
                for k, v in self.explicit_aliases.items()
            }

            file_threshold = config.get("fuzzy_threshold")
            if file_threshold is not None:
                self.fuzzy_threshold = int(file_threshold)

            logger.info(
                f"Loaded {len(self.explicit_aliases)} column aliases "
                f"(fuzzy threshold: {self.fuzzy_threshold})"
            )
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Gagal membaca column mapping file: {e}")

    def _get_column_mapping(
        self, source_columns, dataset_id: str = ""
    ) -> Dict[str, str]:
        """
        Resolve mapping untuk sekumpulan kolom source.
        Hasilnya di-cache per dataset_id.
        """
        source_cols = list(source_columns)
        cache_key = dataset_id or "|".join(sorted(source_cols))

        if cache_key in self._resolved_cache:
            return self._resolved_cache[cache_key]

        mapping: Dict[str, str] = {}
        known_targets: Set[str] = set(self.target_columns)

        for col in source_cols:
            col_lower = col.strip().lower()

            # 1. Explicit alias match (prioritas utama — dari column_mapping.json)
            if col_lower in self.explicit_aliases:
                new_name = self.explicit_aliases[col_lower]
                mapping[col] = new_name
                self._log_rename(dataset_id, col, new_name, "explicit_alias", 100)
                continue

            # 1b. Kolom sudah sesuai standar (muncul sebagai VALUE di alias dict)
            #     Contoh: 'jumlah' sudah standar → tidak perlu rename
            alias_targets = set(self.explicit_aliases.values())
            if col_lower in alias_targets:
                # Sudah standar — hanya lowercase jika perlu
                if col != col_lower:
                    mapping[col] = col_lower
                    self._log_rename(dataset_id, col, col_lower, "case_normalize", 100)
                continue

            # 2. Exact match terhadap known target columns
            if known_targets and col_lower in known_targets:
                # Sudah sesuai standar — hanya perlu lowercase jika perlu
                if col != col_lower:
                    mapping[col] = col_lower
                    self._log_rename(dataset_id, col, col_lower, "case_normalize", 100)
                continue

            # 3. Fuzzy match terhadap known target columns
            if known_targets:
                best_match, best_score = self._fuzzy_find(col_lower, known_targets)
                if best_match and best_score >= self.fuzzy_threshold:
                    mapping[col] = best_match
                    self._log_rename(dataset_id, col, best_match, "fuzzy_match", best_score)
                    continue

            # 4. Fuzzy match terhadap explicit alias values (target standard names)
            alias_targets = set(self.explicit_aliases.values())
            if alias_targets:
                best_match, best_score = self._fuzzy_find(col_lower, alias_targets)
                if best_match and best_score >= self.fuzzy_threshold:
                    mapping[col] = best_match
                    self._log_rename(dataset_id, col, best_match, "fuzzy_alias_target", best_score)
                    continue

            # 5. Tidak bisa di-map → lowercase saja (normalisasi minimal)
            if col != col_lower:
                mapping[col] = col_lower
                self._log_rename(dataset_id, col, col_lower, "lowercase_fallback", 0)

        self._resolved_cache[cache_key] = mapping

        if mapping:
            renamed_pairs = ", ".join(f"'{k}'→'{v}'" for k, v in mapping.items())
            logger.info(f"Dataset {dataset_id}: kolom di-rename: {renamed_pairs}")

        return mapping

    def _fuzzy_find(self, source: str, candidates: Set[str]) -> tuple:
        """Cari kandidat terbaik menggunakan fuzzy matching."""
        best_match = None
        best_score = 0
        for candidate in candidates:
            score = fuzz.ratio(source, candidate)
            if score > best_score:
                best_score = score
                best_match = candidate
        return best_match, best_score

    def _log_rename(
        self, dataset_id: str, old_col: str, new_col: str, method: str, score: int
    ) -> None:
        """Catat setiap rename yang dilakukan."""
        self._rename_log.append({
            "dataset_id": dataset_id,
            "old_column": old_col,
            "new_column": new_col,
            "match_method": method,
            "match_score": score,
        })
