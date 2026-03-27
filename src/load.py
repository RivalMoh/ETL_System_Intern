from typing import Dict, Iterable, Optional, Set

import pandas as pd


class LoadGate:
    """
    Gate data rows for loading based on migration status.
    Default policy only allows rows marked as "ready".
    """

    def __init__(self, allowed_statuses: Optional[Iterable[str]] = None):
        statuses = allowed_statuses if allowed_statuses is not None else ["ready"]
        normalized_statuses = {
            str(status).strip().lower() for status in statuses if str(status).strip()
        }
        if not normalized_statuses:
            raise ValueError("allowed_statuses must contain at least one value")
        self.allowed_statuses: Set[str] = normalized_statuses

    def select_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return a copy of rows that are eligible for load."""
        if "migration_status" not in df.columns:
            raise ValueError("DataFrame must contain 'migration_status' column")

        statuses = df["migration_status"].astype("string").fillna("").str.lower()
        is_allowed = statuses.isin(self.allowed_statuses)
        return df.loc[is_allowed].copy()

    def build_summary(self, df: pd.DataFrame) -> Dict[str, int]:
        """Build a simple load eligibility summary for reporting."""
        loadable_df = self.select_rows(df)
        total_rows = len(df)
        loadable_rows = len(loadable_df)

        return {
            "total_rows": total_rows,
            "loadable_rows": loadable_rows,
            "blocked_rows": total_rows - loadable_rows,
        }
