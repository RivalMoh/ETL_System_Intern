import logging
import os
from collections import defaultdict
from typing import Dict, List

import pandas as pd

from src.config import AppSettings
from src.loader.client import TargetAPIClient
from src.loader.mapper import AutoMapper
from src.loader.progress_tracker import (
    MigrationProgressTracker,
    STATUS_DONE,
    STATUS_PARTIAL,
    STATUS_FAILED,
)
from src.loader.transform import MigrationTransformer

logger = logging.getLogger(__name__)


class MigrationLoadPipeline:
    def __init__(self, settings: AppSettings):
        self.settings = settings
        self.client = TargetAPIClient(settings.new_base_url, settings.new_api_key)

    def run(self, ready_csv_path: str) -> None:
        logger.info("=== MEMULAI FASE MIGRASI (STATEFUL BATCH LOAD) ===")

        # ── 1. Inisialisasi Progress Tracker ─────────────────────────────────
        tracker = MigrationProgressTracker()
        batch_number = tracker.get_next_batch_number()
        logger.info(f"Batch #{batch_number} dimulai.")

        # ── 2. Validasi & Baca File Ready ────────────────────────────────────
        if not os.path.exists(ready_csv_path):
            logger.error(f"File Load Ready tidak ditemukan: {ready_csv_path}")
            return

        df_ready = pd.read_csv(ready_csv_path)
        required_cols = {"Dataset_Id", "Judul_Tabel", "Row_Data_JSON"}
        missing_cols = required_cols - set(df_ready.columns)
        if missing_cols:
            logger.error(
                f"File '{ready_csv_path}' tidak memiliki kolom: {missing_cols}. "
                f"Pastikan file berasal dari output audit pipeline."
            )
            return

        # ── 3. Ambil Katalog Target & Hitung Remaining ───────────────────────
        new_catalog = self.client.get_catalog()
        if not new_catalog:
            logger.error("Gagal mengambil katalog dari sistem baru. Migrasi dibatalkan.")
            self.client.close()
            return

        done_ids = tracker.get_done_ids()
        remaining_catalog = [
            item for item in new_catalog
            if str(item.get("id")) not in done_ids
        ]

        # ── 4. Log Status Lengkap Katalog ─────────────────────────────────────
        tracker.log_catalog_status(new_catalog)
        logger.info(
            f"Batch #{batch_number} | Total katalog: {len(new_catalog)} "
            f"| Sudah done: {len(done_ids)} "
            f"| Sisa (akan diproses): {len(remaining_catalog)}"
        )

        if not remaining_catalog:
            logger.info("Semua tabel di katalog target sudah selesai dimigrasikan.")
            self.client.close()
            return

        # ── 5. AutoMap hanya ke Remaining Catalog ────────────────────────────
        mapper = AutoMapper(threshold=self.settings.dup_threshold)
        df_mapping = mapper.generate_mapping(df_ready, remaining_catalog)

        if df_mapping.empty:
            logger.warning(
                "Tidak ada kecocokan judul antara data siap load dan katalog yang tersisa. "
                "Kemungkinan semua tabel yang ada datanya sudah selesai dimigrasikan."
            )
            self.client.close()
            return

        # ── 6. Transformasi Payload ───────────────────────────────────────────
        transformer = MigrationTransformer(df_mapping)
        payloads = transformer.build_payloads(df_ready)

        if not payloads:
            logger.warning("Tidak ada payload yang berhasil dibangun dari mapping yang ada.")
            self.client.close()
            return

        # ── 7. Kelompokkan Payload per Target ID ─────────────────────────────
        payloads_by_target: Dict[int, List] = defaultdict(list)
        for p in payloads:
            payloads_by_target[p["target_id"]].append(p)

        # Buat lookup mapping info
        mapping_lookup = df_mapping.set_index("new_id")

        # ── 8. Kirim per Dataset & Catat Status ──────────────────────────────
        logger.info(
            f"Memulai pengiriman untuk {len(payloads_by_target)} dataset "
            f"({len(payloads)} payload total)..."
        )

        failed_payloads = []
        batch_counts = {STATUS_DONE: 0, STATUS_PARTIAL: 0, STATUS_FAILED: 0}

        for target_id, target_payloads in payloads_by_target.items():
            # Ambil info mapping untuk logging
            try:
                mapping_row = mapping_lookup.loc[target_id]
                old_id = mapping_row["old_id"]
                new_title = mapping_row["new_title"]
            except KeyError:
                old_id, new_title = "unknown", "unknown"

            success_count = 0
            fail_count = 0
            total_rows_sent = 0

            for payload_item in target_payloads:
                ok = self.client.post_data(payload_item["target_id"], payload_item["body"])
                if ok:
                    success_count += 1
                    total_rows_sent += len(payload_item["body"].get("data", []))
                else:
                    fail_count += 1
                    failed_payloads.append(payload_item)

            # Tentukan status berdasarkan hasil
            if fail_count == 0:
                status = STATUS_DONE
            elif success_count == 0:
                status = STATUS_FAILED
            else:
                status = STATUS_PARTIAL  # sebagian tahun gagal → retry di batch berikutnya

            batch_counts[status] += 1

            tracker.record(
                new_id=target_id,
                new_title=new_title,
                old_id=old_id,
                status=status,
                rows_sent=total_rows_sent,
                batch_number=batch_number,
            )

            logger.info(
                f"[{status.upper():<7}] ID={target_id} | "
                f"{new_title} | {success_count}/{len(target_payloads)} tahun berhasil "
                f"| {total_rows_sent} rows"
            )

        # ── 9. Simpan Failed Payloads ─────────────────────────────────────────
        if failed_payloads:
            failed_path = f"data/reports/failed_payloads_batch{batch_number}.csv"
            pd.DataFrame(failed_payloads).to_csv(failed_path, index=False)
            logger.warning(f"Payload gagal disimpan di: {failed_path}")

        # ── 10. Log Ringkasan Batch ───────────────────────────────────────────
        summary = tracker.get_summary()
        remaining_after = len(new_catalog) - summary["done"]

        logger.info(
            f"=== BATCH #{batch_number} SELESAI "
            f"Done: {batch_counts[STATUS_DONE]} "
            f"Partial: {batch_counts[STATUS_PARTIAL]} "
            f"Gagal: {batch_counts[STATUS_FAILED]} ==="
        )
        logger.info(
            f"PROGRESS TOTAL: {summary['done']}/{len(new_catalog)} tabel selesai "
            f"| {remaining_after} tabel tersisa "
            f"| Jalankan ulang untuk melanjutkan."
        )

        self.client.close()