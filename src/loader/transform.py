import json
import logging
import pandas as pd
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


class MigrationTranformer:
    """
    Bertanggung jawab untuk mengubah data hasil audit menjadi format payload yang siap dikirim ke API target.
    1. harus mempunya df_mapping yang berisi mapping antara old_id dan new_id
    2. menerima df_ready yang merupakan hasil filter dari audit report yang sudah siap untuk diload
    3. menghasilkan list of payloads dengan format {"target_id": 23, "body": {"tahun_data": 2025, "data": [...]}}
    """

    def __init__(self, df_mapping: pd.DataFrame):
        # asumsi mapping punya kolom "old_id" dan "new_id"
        self.mapping = dict(
            zip(df_mapping["old_id"].astype(str), df_mapping["new_id"].astype(int))
        )

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
            data_by_year = {}
            for row in raw_rows:
                # Cari key tahun (bisa "tahun" atau "tahun_data" dari sistem lama)
                tahun = int(row.pop("tahun", row.pop("tahun_data", 0)))

                if tahun == 0:
                    logger.warning(
                        f"Baris dengan Dataset_Id {old_id} tidak memiliki informasi tahun, melewati baris ini."
                    )
                    continue

                if tahun not in data_by_year:
                    data_by_year[tahun] = []

                data_by_year[tahun].append(row)
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
