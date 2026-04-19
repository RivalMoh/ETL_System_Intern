"""
DataPreprocessor — Membersihkan dan menormalisasi data SEBELUM tahap assessment.

Tanggung jawab:
1. Column rename    → berdasarkan column_mapping.json (+ hardcoded tahun fallback)
2. Whitespace strip → collapse multi-space, strip leading/trailing
3. kode_wilayah fix → format BPS: '3320' → '33.20', '332001' → '33.20.01'

Dirancang sebagai fluent API sehingga bisa di-chain:
    df_clean = (DataPreprocessor(df)
                .normalize_columns()
                .strip_whitespace()
                .fix_kode_wilayah()
                .get_result())
"""
import json
import logging
import os
import re
from typing import Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

_DEFAULT_MAPPING_FILE = "data/column_mapping.json"

# Hardcoded tahun patterns sebagai fallback safety
# (jika column_mapping.json belum di-update)
_TAHUN_PATTERNS = [
    "tahun_data",
    "tahundata",
    "tahun_pembuatan",
    "thn",
    "year",
]


class DataPreprocessor:
    """
    Membersihkan DataFrame mentah dari API sebelum masuk ke DataAssessor.
    Semua operasi immutable — bekerja pada copy, tidak mutasi input.
    """

    def __init__(
        self,
        df: pd.DataFrame,
        mapping_file: str = _DEFAULT_MAPPING_FILE,
    ):
        self.df = df.copy()
        self.changes_log: List[Dict[str, str]] = []
        self._aliases: Dict[str, str] = {}
        self._load_mapping(mapping_file)

    # ─── Public API (fluent) ─────────────────────────────────────────────────

    def normalize_columns(self) -> "DataPreprocessor":
        """
        Rename kolom berdasarkan column_mapping.json.
        Prioritas:
        1. Explicit alias dari JSON
        2. Hardcoded tahun patterns (fallback)
        3. Lowercase normalization (jika uppercase)
        """
        rename_map: Dict[str, str] = {}
        used_targets: set = set()  # cegah duplikat target (misal 2 kolom -> 'tahun')

        for col in self.df.columns:
            col_lower = str(col).strip().lower()

            # 1. Explicit alias
            if col_lower in self._aliases:
                target = self._aliases[col_lower]
                if col != target:
                    if target in used_targets or target in self.df.columns:
                        logger.warning(
                            f"Skip rename '{col}' -> '{target}': "
                            f"kolom target sudah ada."
                        )
                        continue
                    rename_map[col] = target
                    used_targets.add(target)
                    self._log("column_rename", col, target, "explicit_alias")
                continue

            # 2. Hardcoded tahun fallback (hanya jika belum ada kolom 'tahun')
            if col_lower in _TAHUN_PATTERNS:
                if "tahun" not in self.df.columns and "tahun" not in used_targets:
                    rename_map[col] = "tahun"
                    used_targets.add("tahun")
                    self._log("column_rename", col, "tahun", "tahun_fallback")
                continue

            # 3. Lowercase normalization
            if col != col_lower:
                alias_targets = set(self._aliases.values())
                if col_lower in alias_targets or col_lower not in self._aliases:
                    if col_lower not in used_targets:
                        rename_map[col] = col_lower
                        used_targets.add(col_lower)
                        self._log("column_rename", col, col_lower, "lowercase")

        if rename_map:
            self.df.rename(columns=rename_map, inplace=True)
            logger.info(
                "Kolom di-rename: %s",
                ", ".join(f"{k} -> {v}" for k, v in rename_map.items()),
            )

        return self

    def strip_whitespace(self) -> "DataPreprocessor":
        """
        Untuk semua kolom string:
        - Strip leading/trailing whitespace
        - Collapse multiple spaces menjadi 1 space
        """
        changed_count = 0
        for col in self.df.columns:
            # Gunakan dtypes dict untuk menghindari error jika ada kolom duplikat
            col_dtype = self.df[col].dtypes if isinstance(self.df[col], pd.DataFrame) else self.df[col].dtype
            if col_dtype == "object" or str(col_dtype) == "object":
                original = self.df[col].copy()
                # Strip + collapse
                self.df[col] = (
                    self.df[col]
                    .astype(str)
                    .str.strip()
                    .str.replace(r"\s+", " ", regex=True)
                )
                # Kembalikan NaN yang aslinya memang NaN
                self.df.loc[original.isna(), col] = None

                diff_count = int((original.fillna("") != self.df[col].fillna("")).sum())
                if diff_count > 0:
                    changed_count += diff_count
                    self._log("whitespace_strip", col, f"{diff_count} values cleaned", "strip")

        if changed_count > 0:
            logger.info(f"Whitespace normalization: {changed_count} nilai dibersihkan.")
        return self

    def fix_kode_wilayah(self, column: str = "kode_wilayah") -> "DataPreprocessor":
        """
        Perbaiki format kode wilayah berdasarkan standar BPS:
        - 4 digit  → XX.XX        (kab/kota)
        - 6 digit  → XX.XX.XX     (kecamatan)
        - 8+ digit → XX.XX.XX.XXXX (kelurahan/desa)

        Jika sudah mengandung titik → skip.
        Jika non-numeric → skip + warning.
        """
        if column not in self.df.columns:
            logger.debug(f"Kolom '{column}' tidak ditemukan, skip fix_kode_wilayah.")
            return self

        fixed_count = 0
        for idx, val in self.df[column].items():
            if pd.isna(val):
                continue

            val_str = str(val).strip()

            # Sudah punya titik → format kemungkinan benar, skip
            if "." in val_str:
                continue

            # Hapus non-digit untuk cek apakah pure numeric
            digits = re.sub(r"\D", "", val_str)
            if not digits or digits != val_str:
                # Non-numeric atau mixed → skip + warning
                if val_str:
                    logger.warning(
                        f"kode_wilayah baris {idx}: '{val_str}' bukan format numerik, dilewati."
                    )
                continue

            # Format berdasarkan panjang digit
            formatted = self._format_kode_wilayah(digits)
            if formatted and formatted != val_str:
                self.df.at[idx, column] = formatted
                fixed_count += 1

        if fixed_count > 0:
            self._log("kode_wilayah_fix", column, f"{fixed_count} values fixed", "format")
            logger.info(
                f"kode_wilayah: {fixed_count} nilai diperbaiki formatnya."
            )
        return self

    def get_result(self) -> pd.DataFrame:
        """Kembalikan DataFrame yang sudah dibersihkan."""
        return self.df

    def get_changes_log(self) -> List[Dict[str, str]]:
        """Kembalikan log semua perubahan yang dilakukan."""
        return self.changes_log

    # ─── Private ─────────────────────────────────────────────────────────────

    def _load_mapping(self, mapping_file: str) -> None:
        """Load column aliases dari file JSON."""
        if not os.path.exists(mapping_file):
            logger.warning(
                f"File column mapping tidak ditemukan: {mapping_file}. "
                f"Hanya hardcoded patterns yang digunakan."
            )
            return

        try:
            with open(mapping_file, "r", encoding="utf-8") as f:
                config = json.load(f)

            raw_aliases = config.get("column_aliases", {})
            self._aliases = {
                k.strip().lower(): v.strip().lower()
                for k, v in raw_aliases.items()
            }
            logger.info(f"Loaded {len(self._aliases)} column aliases dari {mapping_file}")
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Gagal membaca column mapping file: {e}")

    @staticmethod
    def _format_kode_wilayah(digits: str) -> Optional[str]:
        """
        Format digit kode wilayah sesuai standar BPS.

        Aturan:
        - 4 digit → XX.XX
        - 6 digit → XX.XX.XX
        - 8+ digit → XX.XX.XX.XXXX (sisa digit jadi bagian terakhir)
        - < 4 digit → None (tidak bisa diformat)
        """
        n = len(digits)
        if n < 4:
            return None  # terlalu pendek, tidak bisa diformat

        # Selalu mulai dengan XX.XX (provinsi.kab_kota)
        result = f"{digits[:2]}.{digits[2:4]}"

        if n >= 6:
            # Tambah kecamatan
            result += f".{digits[4:6]}"

        if n >= 8:
            # Tambah kelurahan/desa
            result += f".{digits[6:]}"

        return result

    def _log(self, action: str, target: str, detail: str, method: str) -> None:
        self.changes_log.append({
            "action": action,
            "target": target,
            "detail": detail,
            "method": method,
        })
