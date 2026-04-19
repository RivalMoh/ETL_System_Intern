import logging
import os
import pandas as pd
from datetime import datetime
from typing import Any, Dict, List, Set

logger = logging.getLogger(__name__)

_PROGRESS_COLUMNS = [
    "new_id",
    "new_title",
    "old_id",
    "status",
    "rows_sent",
    "migrated_at",
    "batch_number",
]

# Status constants
STATUS_DONE = "done"
STATUS_PARTIAL = "partial"
STATUS_FAILED = "failed"


class MigrationProgressTracker:
    """
    Melacak status migrasi per tabel secara persisten antar batch.

    Status per tabel:
    - "done"    → semua tahun berhasil dikirim, tidak akan diproses lagi.
    - "partial" → sebagian tahun gagal, akan dicoba ulang di batch berikutnya.
    - "failed"  → semua tahun gagal, akan dicoba ulang di batch berikutnya.

    Hanya tabel dengan status "done" yang di-skip pada batch selanjutnya.
    """

    def __init__(self, progress_file: str = "data/reports/migration_progress.csv"):
        self.progress_file = progress_file
        _dir = os.path.dirname(progress_file)
        if _dir:
            os.makedirs(_dir, exist_ok=True)
        self._df = self._load()

    # ─── Public API ──────────────────────────────────────────────────────────

    def get_done_ids(self) -> Set[str]:
        """Kembalikan set new_id yang sudah berstatus 'done'."""
        done_mask = self._df["status"] == STATUS_DONE
        return set(self._df.loc[done_mask, "new_id"].astype(str).tolist())

    def get_next_batch_number(self) -> int:
        """Auto-increment dari batch terakhir yang tercatat."""
        if self._df.empty or self._df["batch_number"].isna().all():
            return 1
        try:
            return int(self._df["batch_number"].max()) + 1
        except (ValueError, TypeError):
            return 1

    def record(
        self,
        new_id: Any,
        new_title: str,
        old_id: Any,
        status: str,
        rows_sent: int = 0,
        batch_number: int = 1,
    ) -> None:
        """
        Upsert satu baris progress.
        Jika new_id sudah ada → update statusnya.
        Jika belum ada → tambah baris baru.
        """
        new_row: Dict[str, Any] = {
            "new_id": str(new_id),
            "new_title": new_title,
            "old_id": str(old_id),
            "status": status,
            "rows_sent": rows_sent,
            "migrated_at": datetime.now().isoformat(timespec="seconds"),
            "batch_number": batch_number,
        }

        # Drop-then-append: aman terhadap duplicate index labels
        mask = self._df["new_id"].astype(str) == str(new_id)
        if mask.any():
            self._df = self._df[~mask]

        self._df = pd.concat(
            [self._df, pd.DataFrame([new_row])], ignore_index=True
        )

        self._save()

    def get_summary(self) -> Dict[str, int]:
        """Kembalikan ringkasan statistik dari progress file."""
        return {
            "total_recorded": len(self._df),
            "done": int((self._df["status"] == STATUS_DONE).sum()),
            "partial": int((self._df["status"] == STATUS_PARTIAL).sum()),
            "failed": int((self._df["status"] == STATUS_FAILED).sum()),
        }

    def log_catalog_status(self, new_catalog: List[Dict[str, Any]]) -> None:
        """
        Log status setiap tabel di katalog target:
        [DONE] → sudah selesai
        [PARTIAL] / [FAILED] → pernah dicoba, belum selesai
        [NEW] → belum pernah diproses
        """
        done_ids = self.get_done_ids()
        id_to_title = {
            str(item.get("id")): str(item.get("judul", ""))
            for item in new_catalog
        }

        done_list = [tid for tid in id_to_title if tid in done_ids]
        pending_list = [tid for tid in id_to_title if tid not in done_ids]

        separator = "-" * 60
        logger.info(separator)
        logger.info(
            f"STATUS KATALOG TARGET | Total: {len(new_catalog)} tabel "
            f"| Done: {len(done_list)} | Sisa: {len(pending_list)}"
        )
        logger.info(separator)

        for tid in done_list:
            logger.info(f"  [DONE]    ID={tid:>6} | {id_to_title[tid]}")

        for tid in pending_list:
            mask = self._df["new_id"].astype(str) == tid
            if mask.any():
                status = self._df.loc[mask, "status"].iloc[0].upper()
                logger.info(f"  [{status:<7}] ID={tid:>6} | {id_to_title[tid]}")
            else:
                logger.info(f"  [NEW]     ID={tid:>6} | {id_to_title[tid]}")

        logger.info(separator)

    # ─── Private ─────────────────────────────────────────────────────────────

    def _load(self) -> pd.DataFrame:
        if os.path.exists(self.progress_file):
            df = pd.read_csv(self.progress_file, dtype={"new_id": str, "old_id": str})
            # Pastikan semua kolom ada (backward-compat jika file lama)
            for col in _PROGRESS_COLUMNS:
                if col not in df.columns:
                    df[col] = None
            return df[_PROGRESS_COLUMNS]
        return pd.DataFrame(columns=_PROGRESS_COLUMNS)

    def _save(self) -> None:
        self._df.to_csv(self.progress_file, index=False)
