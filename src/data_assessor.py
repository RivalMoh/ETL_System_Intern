import logging
from typing import Iterable, List

import pandas as pd

logger = logging.getLogger(__name__)


class DataAssessor:
    STATUS_PENDING = "pending"
    STATUS_READY = "ready"
    STATUS_FLAGGED = "flagged"

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.assessment_issues: List[str] = []

        if "migration_status" not in self.df.columns:
            self.df["migration_status"] = self.STATUS_PENDING
        if "flag_reason" not in self.df.columns:
            self.df["flag_reason"] = ""

        # Normalize status and reason columns
        self.df["migration_status"] = (
            self.df["migration_status"]
            .astype("string")
            .fillna(self.STATUS_PENDING)
            .str.lower()
        )
        self.df["flag_reason"] = self.df["flag_reason"].fillna("").astype("string")

    def flag_missing_values(self, required_columns: Iterable[str]):
        """
        Flag rows with missing values for required columns.
        Missing columns are captured as assessment issues (schema-level), not row-level flags.
        """
        required_columns = list(required_columns)
        missing_columns = [
            col for col in required_columns if col not in self.df.columns
        ]

        if missing_columns:
            issue = f"Missing required columns: {missing_columns}"
            self.assessment_issues.append(issue)
            logger.warning(issue)

        for col in required_columns:
            if col not in self.df.columns:
                continue

            col_data = self.df[col]
            if isinstance(col_data, pd.DataFrame):
                logger.warning(
                    f"Skema kotor: Ditemukan {col_data.shape[1]} kolom bernama '{col}'. Hanya mengevaluasi yang pertama."
                )
                col_data = col_data.iloc[:, 0]

            # Treat None, NaN, and empty/whitespace string as missing
            as_string = self.df[col].astype("string")
            missing_mask = self.df[col].isna().fillna(False) | (
                as_string.str.strip() == ""
            )
            self._update_flags(missing_mask, f"Missing '{col}'")

        return self

    def flag_duplicates(self, subset: Iterable[str]):
        """
        Flag duplicate rows by subset columns.
        If subset columns are missing, record issue and skip safely.
        """
        subset = list(subset)
        missing_subset = [col for col in subset if col not in self.df.columns]
        if missing_subset:
            issue = f"Duplicate check skipped, missing columns: {missing_subset}"
            self.assessment_issues.append(issue)
            logger.warning(issue)
            return self

        duplicate_mask = self.df.duplicated(subset=subset, keep=False)
        self._update_flags(duplicate_mask, f"Duplicate subset: {subset}")
        return self

    def apply_custom_rule(self, condition_mask: pd.Series, reason: str):
        if not isinstance(condition_mask, pd.Series):
            raise TypeError("condition_mask must be a pandas Series")

        if len(condition_mask) != len(self.df):
            raise ValueError("condition_mask length must match DataFrame length")

        aligned_mask = condition_mask.reindex(self.df.index, fill_value=False).astype(
            bool
        )
        self._update_flags(aligned_mask, reason)
        return self

    def warn_suspicious_year(
        self,
        min_year: int = 2000,
        max_year: int = 2025,
        year_column: str = "tahun",
    ):
        """
        Tandai baris dengan tahun di luar range [min_year, max_year] sebagai WARNING.
        Data TETAP DIPASS (status TIDAK berubah), tapi flag_reason ditambah warning text.

        Berbeda dari flag_*() methods yang mengubah migration_status → flagged.
        """
        if year_column not in self.df.columns:
            logger.debug(
                f"Kolom '{year_column}' tidak ditemukan, skip warn_suspicious_year."
            )
            return self

        # Konversi ke numeric, non-numeric jadi NaN
        year_numeric = pd.to_numeric(self.df[year_column], errors="coerce")

        # Deteksi di luar range (abaikan NaN — sudah ditangani oleh flag_missing_values)
        too_low = year_numeric.notna() & (year_numeric < min_year)
        too_high = year_numeric.notna() & (year_numeric > max_year)

        suspicious_mask = too_low | too_high

        if suspicious_mask.any():
            count = int(suspicious_mask.sum())
            logger.warning(
                f"Ditemukan {count} baris dengan tahun di luar range "
                f"[{min_year}-{max_year}]. Baris tetap dipass dengan WARNING."
            )
            # Tambahkan warning ke flag_reason TANPA mengubah migration_status
            warning_text = f"WARNING: tahun di luar range [{min_year}-{max_year}]"
            self.df.loc[suspicious_mask, "flag_reason"] = self.df.loc[
                suspicious_mask, "flag_reason"
            ].apply(lambda x: f"{x} | {warning_text}" if x else warning_text)

        return self

    def mark_ready(self) -> pd.DataFrame:
        """
        Finalize assessment by marking unflagged rows as ready.
        """
        ready_mask = self.df["migration_status"] == self.STATUS_PENDING
        self.df.loc[ready_mask, "migration_status"] = self.STATUS_READY
        return self.df

    def standardize_year_column(self):
        """
        mencari kolom yang merepresentasikan 'tahun' (mengabaikan kapitalisasi)
        dan menstandarkan menjadi nama kolom 'tahun'
        """
        year_patterns = [
            "tahun",
            "tahun_data",
            "year",
            "thn",
            "tahun_pembuatan",
            "tahundata",
        ]
        col_map = {
            col: str(col).lower().strip().replace(" ", "") for col in self.df.columns
        }
        found_year_col = None

        # cari kecocokan persis dari daftar pola
        for orig_col, clean_col in col_map.items():
            if clean_col in year_patterns:
                found_year_col = orig_col
                break
        # jika tidak ditemukan, coba cari yang mengandung kata 'tahun' atau 'year'
        if not found_year_col:
            for orig_col, clean_col in col_map.items():
                if "tahun" in clean_col or "year" in clean_col:
                    found_year_col = orig_col
                    logger.info(f"Menstandarkan kolom '{orig_col}' sebagai 'tahun'")
                    break

        if found_year_col:
            if found_year_col != "tahun":
                if "tahun" in self.df.columns:
                    logger.warning(
                        f"Kolom 'tahun' sudah ada di tabel asal. Mengabaikan normalisasi dari '{found_year_col}'."
                    )
                else:
                    logger.info(
                        f"Menormalisasi kolom '{found_year_col}' menjadi 'tahun'"
                    )
                    self.df.rename(columns={found_year_col: "tahun"}, inplace=True)

        else:
            issue = "Tidak ditemukan kolom tahun yang valid untuk distandarkan."
            self.assessment_issues.append(issue)
            logger.warning(issue)

        return self

    def _update_flags(self, mask: pd.Series, reason: str):
        if not mask.any():
            logger.info("No rows flagged for reason: %s", reason)
            return

        self.df.loc[mask, "flag_reason"] = self.df.loc[mask, "flag_reason"].apply(
            lambda x: f"{x} | {reason}" if x else reason
        )
        self.df.loc[mask, "migration_status"] = self.STATUS_FLAGGED
        logger.info("Flagged %s rows for reason: %s", int(mask.sum()), reason)
