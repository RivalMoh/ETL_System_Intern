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

            # Treat None, NaN, and empty/whitespace string as missing
            as_string = self.df[col].astype("string")
            missing_mask = self.df[col].isna() | (as_string.str.strip() == "")
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

    def mark_ready(self) -> pd.DataFrame:
        """
        Finalize assessment by marking unflagged rows as ready.
        """
        ready_mask = self.df["migration_status"] == self.STATUS_PENDING
        self.df.loc[ready_mask, "migration_status"] = self.STATUS_READY
        return self.df

    def _update_flags(self, mask: pd.Series, reason: str):
        if not mask.any():
            logger.info(f"No rows flagged for reason: {reason}")
            return

        self.df.loc[mask, "flag_reason"] = self.df.loc[mask, "flag_reason"].apply(
            lambda x: f"{x} | {reason}" if x else reason
        )
        self.df.loc[mask, "migration_status"] = self.STATUS_FLAGGED
        logger.info(f"Flagged {int(mask.sum())} rows for reason: {reason}")
