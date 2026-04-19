import json
import logging
import pandas as pd
from typing import List, Dict, Any, Optional

from src.loader.column_normalizer import ColumnNormalizer

logger = logging.getLogger(__name__)


class MigrationTransformer:
    """
    Bertanggung jawab untuk mengubah data hasil audit menjadi format payload yang siap dikirim ke API target.
    1. harus mempunya df_mapping yang berisi mapping antara old_id dan new_id
    2. menerima df_ready yang merupakan hasil filter dari audit report yang sudah siap untuk diload
    3. menghasilkan list of payloads dengan format {"target_id": 23, "body": {"tahun_data": 2025, "data": [...]}}
    """

    def __init__(
        self,
        df_mapping: pd.DataFrame,
        column_normalizer: Optional[ColumnNormalizer] = None,
    ):
        # asumsi mapping punya kolom "old_id" dan "new_id"
        self.mapping = dict(
            zip(df_mapping["old_id"].astype(str), df_mapping["new_id"].astype(int))
        )
        self.normalizer = column_normalizer

    def build_payloads(self, df_ready: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Mengubah dataframe hasil audit menjadi list of payloads siap POST.
        Format Payload: {"target_id": 23, "body": {"tahun_data": 2025, "data": [...]}}
        """
        payloads_to_send = []

        # Kelompokkan data berdasarkan ID dataset lama
        for old_id, group in df_ready.groupby("Dataset_Id"):
            target_id = self.mapping.get(str(old_id))

            if not target_id:
                logger.warning(
                    f"Tidak ditemukan mapping untuk Dataset_Id {old_id}, melewati dataset ini."
                )
                continue

            # ekstrak Row_Data_JSON menjadi list of dict
            raw_rows = [json.loads(row) for row in group["Row_Data_JSON"]]

            # kelompokkan baris berdasarkan Tahun
            data_by_year: Dict[int, list] = {}
            for row in raw_rows:
                # ── Normalisasi kolom sebelum proses ──────────────────────
                if self.normalizer:
                    row = self.normalizer.normalize_record(row, dataset_id=str(old_id))

                # Ambil nilai tahun tanpa mutasi dict asli
                tahun_raw = row.get("tahun")

                if tahun_raw is None:
                    logger.warning(
                        f"Dataset_Id {old_id}: baris tidak punya key 'tahun', dilewati."
                    )
                    continue

                try:
                    tahun = int(float(tahun_raw))
                    if tahun == 0:
                        raise ValueError("tahun bernilai 0")
                except (ValueError, TypeError):
                    logger.warning(
                        f"Dataset_Id {old_id}: nilai tahun '{tahun_raw}' tidak valid, dilewati."
                    )
                    continue

                # Bangun record tanpa kolom tahun (sudah diangkat ke level payload)
                record = {k: v for k, v in row.items() if k != "tahun"}
                data_by_year.setdefault(tahun, []).append(record)
            # Buat struktur payload untuk setiap tahun
            for tahun, records in data_by_year.items():
                payloads_to_send.append(
                    {
                        "target_id": target_id,
                        "body": {"tahun_data": tahun, "data": records},
                    }
                )

        logger.info(f"Total payload siap dikirim: {len(payloads_to_send)}")
        return payloads_to_send
