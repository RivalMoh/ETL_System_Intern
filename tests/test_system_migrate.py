"""
System Tests — Migrate Pipeline (src/loader/pipeline.py)

Menguji MigrationLoadPipeline.run() secara end-to-end dengan menggunakan
fake CSV, mock HTTP client, dan progress tracker di temporary directory.

Setiap test memverifikasi PERILAKU SISTEM SECARA KESELURUHAN:
- Apakah tabel yang sudah 'done' di-skip?
- Apakah progress tracker diperbarui dengan status yang benar?
- Apakah pipeline berhenti dengan benar ketika semua selesai?
"""
import json
import os
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, call

from src.loader.pipeline import MigrationLoadPipeline
from src.loader.progress_tracker import (
    MigrationProgressTracker,
    STATUS_DONE,
    STATUS_PARTIAL,
    STATUS_FAILED,
)
from src.config import AppSettings


# ─── Fixtures & Helpers ───────────────────────────────────────────────────────


def make_settings(**overrides) -> AppSettings:
    settings = AppSettings.__new__(AppSettings)
    settings.base_url = "https://api.old.test/v1"
    settings.api_key = "old-key"
    settings.new_base_url = "https://api.new.test/v1"
    settings.new_api_key = "new-key"
    settings.dup_threshold = 85
    for k, v in overrides.items():
        setattr(settings, k, v)
    return settings


def make_ready_csv(tmp_path, rows: list, filename: str = "ready.csv") -> str:
    """Tulis df_ready ke file CSV sementara dan kembalikan path-nya."""
    df = pd.DataFrame(rows)
    path = str(tmp_path / filename)
    df.to_csv(path, index=False)
    return path


def make_ready_row(dataset_id: str, title: str, tahun: int, **extra) -> dict:
    """Buat satu baris df_ready dengan Row_Data_JSON yang valid."""
    data = {"tahun": tahun, **extra}
    return {
        "Dataset_Id": dataset_id,
        "Judul_Tabel": title,
        "Row_Data_JSON": json.dumps(data),
    }


def make_mock_client(catalog_items: list, post_results: list) -> MagicMock:
    """
    Buat fake TargetAPIClient.
    post_results: list of bool, dipakai secara berurutan per post_data call.
    """
    client = MagicMock()
    client.get_catalog.return_value = catalog_items
    client.post_data.side_effect = post_results
    return client


def make_pipeline(settings, mock_client, tracker) -> MigrationLoadPipeline:
    """Buat MigrationLoadPipeline dengan client dan tracker yang sudah di-inject."""
    pipeline = MigrationLoadPipeline.__new__(MigrationLoadPipeline)
    pipeline.settings = settings
    pipeline.client = mock_client
    pipeline._tracker_override = tracker  # akan dipakai oleh patch
    return pipeline


# ─── Scenario 1: Cold Start (tidak ada progress sebelumnya) ──────────────────


def test_migrate_cold_start_posts_all_datasets(tmp_path):
    """
    Scenario: Cold start, tidak ada progress sebelumnya.
    Expected: Semua dataset dicoba di-POST.
    """
    settings = make_settings()
    ready_csv = make_ready_csv(tmp_path, [
        make_ready_row("old_1", "Data Padi Jawa Tengah", 2022, jumlah=100),
        make_ready_row("old_2", "Data Jagung Jawa Tengah", 2022, jumlah=200),
    ])

    new_catalog = [
        {"id": 10, "judul": "Data Padi Jawa Tengah"},
        {"id": 20, "judul": "Data Jagung Jawa Tengah"},
    ]
    mock_client = make_mock_client(new_catalog, post_results=[True, True])

    progress_file = str(tmp_path / "progress.csv")

    with patch("src.loader.pipeline.TargetAPIClient", return_value=mock_client), \
         patch("src.loader.pipeline.MigrationProgressTracker",
               return_value=MigrationProgressTracker(progress_file)):
        pipeline = MigrationLoadPipeline(settings)
        pipeline.run(ready_csv)

    # Kedua dataset harus di-POST
    assert mock_client.post_data.call_count == 2


def test_migrate_cold_start_records_done_status(tmp_path):
    """
    Setelah cold start sukses, kedua dataset harus tercatat sebagai 'done'
    di progress tracker.
    """
    settings = make_settings()
    ready_csv = make_ready_csv(tmp_path, [
        make_ready_row("old_1", "Data Padi Jawa Tengah", 2022, jumlah=100),
    ])
    new_catalog = [{"id": 10, "judul": "Data Padi Jawa Tengah"}]
    mock_client = make_mock_client(new_catalog, post_results=[True])

    progress_file = str(tmp_path / "progress.csv")
    tracker = MigrationProgressTracker(progress_file)

    with patch("src.loader.pipeline.TargetAPIClient", return_value=mock_client), \
         patch("src.loader.pipeline.MigrationProgressTracker", return_value=tracker):
        pipeline = MigrationLoadPipeline(settings)
        pipeline.run(ready_csv)

    assert "10" in tracker.get_done_ids()


# ─── Scenario 2: Resume Batch (tabel yang sudah done di-skip) ────────────────


def test_migrate_batch2_skips_done_tables(tmp_path):
    """
    Scenario: Batch 1 selesai dengan 1 dataset done.
              Batch 2 dijalankan — dataset done tidak boleh di-POST lagi.
    Expected: post_data dipanggil hanya 1x (bukan 2x).
    """
    settings = make_settings()
    ready_csv = make_ready_csv(tmp_path, [
        make_ready_row("old_1", "Data Padi Jawa Tengah", 2022, jumlah=100),    # sudah done
        make_ready_row("old_2", "Data Jagung Jawa Tengah", 2022, jumlah=200),  # belum done
    ])

    new_catalog = [
        {"id": 10, "judul": "Data Padi Jawa Tengah"},
        {"id": 20, "judul": "Data Jagung Jawa Tengah"},
    ]
    # Hanya 1 POST karena ID=10 sudah done
    mock_client = make_mock_client(new_catalog, post_results=[True])

    progress_file = str(tmp_path / "progress.csv")
    tracker = MigrationProgressTracker(progress_file)
    # Simulasi: batch 1 sudah selesai untuk ID=10
    tracker.record(new_id=10, new_title="Data Padi Jawa Tengah",
                   old_id="old_1", status=STATUS_DONE, rows_sent=1, batch_number=1)

    with patch("src.loader.pipeline.TargetAPIClient", return_value=mock_client), \
         patch("src.loader.pipeline.MigrationProgressTracker", return_value=tracker):
        pipeline = MigrationLoadPipeline(settings)
        pipeline.run(ready_csv)

    # Hanya dataset ke-2 yang di-POST
    assert mock_client.post_data.call_count == 1


def test_migrate_batch2_done_ids_remain_done_after_run(tmp_path):
    """
    Dataset yang sudah 'done' di batch 1 harus tetap 'done' setelah batch 2 selesai.
    Statusnya tidak boleh berubah.
    """
    settings = make_settings()
    ready_csv = make_ready_csv(tmp_path, [
        make_ready_row("old_1", "Data Padi Jawa Tengah", 2022, jumlah=100),
        make_ready_row("old_2", "Data Jagung Jawa Tengah", 2022, jumlah=200),
    ])
    new_catalog = [
        {"id": 10, "judul": "Data Padi Jawa Tengah"},
        {"id": 20, "judul": "Data Jagung Jawa Tengah"},
    ]
    mock_client = make_mock_client(new_catalog, post_results=[True])

    progress_file = str(tmp_path / "progress.csv")
    tracker = MigrationProgressTracker(progress_file)
    tracker.record(10, "Data Padi Jawa Tengah", "old_1", STATUS_DONE, rows_sent=5, batch_number=1)

    with patch("src.loader.pipeline.TargetAPIClient", return_value=mock_client), \
         patch("src.loader.pipeline.MigrationProgressTracker", return_value=tracker):
        MigrationLoadPipeline(settings).run(ready_csv)

    # ID=10 harus masih done!
    reloaded = MigrationProgressTracker(progress_file)
    assert "10" in reloaded.get_done_ids()


# ─── Scenario 3: Semua Sudah Selesai ─────────────────────────────────────────


def test_migrate_all_done_exits_without_posting(tmp_path):
    """
    Jika semua tabel di katalog sudah 'done', tidak ada POST yang dilakukan.
    """
    settings = make_settings()
    ready_csv = make_ready_csv(tmp_path, [
        make_ready_row("old_1", "Data Padi Jawa Tengah", 2022, jumlah=100),
    ])
    new_catalog = [{"id": 10, "judul": "Data Padi Jawa Tengah"}]
    mock_client = make_mock_client(new_catalog, post_results=[])  # tidak diharapkan dipanggil

    progress_file = str(tmp_path / "progress.csv")
    tracker = MigrationProgressTracker(progress_file)
    tracker.record(10, "Data Padi Jawa Tengah", "old_1", STATUS_DONE, rows_sent=5, batch_number=1)

    with patch("src.loader.pipeline.TargetAPIClient", return_value=mock_client), \
         patch("src.loader.pipeline.MigrationProgressTracker", return_value=tracker):
        MigrationLoadPipeline(settings).run(ready_csv)

    mock_client.post_data.assert_not_called()


# ─── Scenario 4: Partial & Failed Status ─────────────────────────────────────


def test_migrate_partial_post_records_partial_status(tmp_path):
    """
    Dataset dengan 2 tahun dimana 1 berhasil dan 1 gagal → status 'partial'.
    """
    settings = make_settings()
    ready_csv = make_ready_csv(tmp_path, [
        make_ready_row("old_1", "Data Padi Jawa Tengah", 2022, jumlah=100),
        make_ready_row("old_1", "Data Padi Jawa Tengah", 2023, jumlah=150),  # tahun kedua
    ])
    new_catalog = [{"id": 10, "judul": "Data Padi Jawa Tengah"}]
    # 2022 sukses, 2023 gagal
    mock_client = make_mock_client(new_catalog, post_results=[True, False])

    progress_file = str(tmp_path / "progress.csv")
    tracker = MigrationProgressTracker(progress_file)

    with patch("src.loader.pipeline.TargetAPIClient", return_value=mock_client), \
         patch("src.loader.pipeline.MigrationProgressTracker", return_value=tracker):
        MigrationLoadPipeline(settings).run(ready_csv)

    # ID=10 harus partial (bukan done, bukan failed)
    reloaded = MigrationProgressTracker(progress_file)
    row = reloaded._df[reloaded._df["new_id"] == "10"]
    assert not row.empty
    assert row.iloc[0]["status"] == STATUS_PARTIAL
    assert "10" not in reloaded.get_done_ids()  # tidak boleh jadi done


def test_migrate_all_failed_post_records_failed_status(tmp_path):
    """
    Dataset dimana semua POST gagal → status 'failed' (bukan 'partial').
    """
    settings = make_settings()
    ready_csv = make_ready_csv(tmp_path, [
        make_ready_row("old_1", "Data Padi Jawa Tengah", 2022, jumlah=100),
    ])
    new_catalog = [{"id": 10, "judul": "Data Padi Jawa Tengah"}]
    mock_client = make_mock_client(new_catalog, post_results=[False])

    progress_file = str(tmp_path / "progress.csv")
    tracker = MigrationProgressTracker(progress_file)

    with patch("src.loader.pipeline.TargetAPIClient", return_value=mock_client), \
         patch("src.loader.pipeline.MigrationProgressTracker", return_value=tracker):
        MigrationLoadPipeline(settings).run(ready_csv)

    reloaded = MigrationProgressTracker(progress_file)
    row = reloaded._df[reloaded._df["new_id"] == "10"]
    assert row.iloc[0]["status"] == STATUS_FAILED


def test_migrate_partial_and_failed_are_retried_in_next_batch(tmp_path):
    """
    Tabel dengan status 'partial' dan 'failed' harus diretry di batch berikutnya
    (keduanya muncul di remaining_catalog).
    """
    settings = make_settings()
    ready_csv = make_ready_csv(tmp_path, [
        make_ready_row("old_1", "Data Padi Jawa Tengah", 2022, jumlah=100),
        make_ready_row("old_2", "Data Jagung Jawa Tengah", 2022, jumlah=200),
    ])
    new_catalog = [
        {"id": 10, "judul": "Data Padi Jawa Tengah"},
        {"id": 20, "judul": "Data Jagung Jawa Tengah"},
    ]
    mock_client = make_mock_client(new_catalog, post_results=[True, True])

    progress_file = str(tmp_path / "progress.csv")
    tracker = MigrationProgressTracker(progress_file)
    # Simulasi batch 1: ID=10 partial, ID=20 failed
    tracker.record(10, "Data Padi Jawa Tengah", "old_1", STATUS_PARTIAL, rows_sent=1, batch_number=1)
    tracker.record(20, "Data Jagung Jawa Tengah", "old_2", STATUS_FAILED, rows_sent=0, batch_number=1)

    with patch("src.loader.pipeline.TargetAPIClient", return_value=mock_client), \
         patch("src.loader.pipeline.MigrationProgressTracker", return_value=tracker):
        MigrationLoadPipeline(settings).run(ready_csv)

    # Kedua-duanya harus dicoba ulang (post_data dipanggil 2x)
    assert mock_client.post_data.call_count == 2
    # Setelah sukses, keduanya harus menjadi done
    reloaded = MigrationProgressTracker(progress_file)
    assert reloaded.get_done_ids() == {"10", "20"}


# ─── Scenario 5: Error & Edge Cases ─────────────────────────────────────────


def test_migrate_missing_ready_file_exits_gracefully(tmp_path):
    """
    Jika ready_csv_path tidak ada, pipeline harus berhenti tanpa crash.
    post_data tidak boleh dipanggil.
    """
    settings = make_settings()
    new_catalog = [{"id": 10, "judul": "Data Padi Jawa Tengah"}]
    mock_client = make_mock_client(new_catalog, post_results=[])

    progress_file = str(tmp_path / "progress.csv")

    with patch("src.loader.pipeline.TargetAPIClient", return_value=mock_client), \
         patch("src.loader.pipeline.MigrationProgressTracker",
               return_value=MigrationProgressTracker(progress_file)):
        MigrationLoadPipeline(settings).run("/non_existent/path/ready.csv")

    mock_client.post_data.assert_not_called()


def test_migrate_missing_required_columns_exits_gracefully(tmp_path):
    """
    Jika file CSV tidak punya kolom Dataset_Id/Judul_Tabel/Row_Data_JSON,
    pipeline harus berhenti tanpa crash.
    """
    settings = make_settings()
    # CSV dengan kolom yang salah
    bad_csv = str(tmp_path / "bad.csv")
    pd.DataFrame([{"kolom_salah": "nilai"}]).to_csv(bad_csv, index=False)

    new_catalog = [{"id": 10, "judul": "Data Padi"}]
    mock_client = make_mock_client(new_catalog, post_results=[])

    progress_file = str(tmp_path / "progress.csv")

    with patch("src.loader.pipeline.TargetAPIClient", return_value=mock_client), \
         patch("src.loader.pipeline.MigrationProgressTracker",
               return_value=MigrationProgressTracker(progress_file)):
        MigrationLoadPipeline(settings).run(bad_csv)

    mock_client.post_data.assert_not_called()


def test_migrate_empty_catalog_from_target_exits_gracefully(tmp_path):
    """
    Jika get_catalog() mengembalikan list kosong (server down / kosong),
    pipeline harus berhenti tanpa crash.
    """
    settings = make_settings()
    ready_csv = make_ready_csv(tmp_path, [
        make_ready_row("old_1", "Data Padi Jateng", 2022),
    ])
    mock_client = make_mock_client(catalog_items=[], post_results=[])

    progress_file = str(tmp_path / "progress.csv")

    with patch("src.loader.pipeline.TargetAPIClient", return_value=mock_client), \
         patch("src.loader.pipeline.MigrationProgressTracker",
               return_value=MigrationProgressTracker(progress_file)):
        MigrationLoadPipeline(settings).run(ready_csv)

    mock_client.post_data.assert_not_called()


def test_migrate_batch_number_increments_between_runs(tmp_path):
    """
    Setiap kali pipeline dijalankan, batch_number di progress file harus bertambah.
    """
    settings = make_settings()

    new_catalog_run1 = [{"id": 10, "judul": "Data Padi Jawa Tengah"}]
    ready_csv1 = make_ready_csv(tmp_path, [
        make_ready_row("old_1", "Data Padi Jawa Tengah", 2022, jumlah=100),  # ← judul identik
    ], filename="ready1.csv")

    progress_file = str(tmp_path / "progress.csv")

    # ── Run 1 ─────────────────────────────────────────────────────────────────
    mock_client1 = make_mock_client(new_catalog_run1, post_results=[True])
    tracker1 = MigrationProgressTracker(progress_file)
    with patch("src.loader.pipeline.TargetAPIClient", return_value=mock_client1), \
         patch("src.loader.pipeline.MigrationProgressTracker", return_value=tracker1):
        MigrationLoadPipeline(settings).run(ready_csv1)

    df_after_run1 = MigrationProgressTracker(progress_file)._df
    assert not df_after_run1.empty, "Run 1 seharusnya merekam entry di progress file"
    batch1 = int(df_after_run1[df_after_run1["new_id"] == "10"]["batch_number"].iloc[0])

    # ── Run 2: katalog lebih luas, 1 dataset baru ─────────────────────────────
    new_catalog_run2 = [
        {"id": 10, "judul": "Data Padi Jawa Tengah"},   # sudah done → di-skip
        {"id": 20, "judul": "Data Jagung Jawa Tengah"}, # baru → diproses
    ]
    ready_csv2 = make_ready_csv(tmp_path, [
        make_ready_row("old_2", "Data Jagung Jawa Tengah", 2022, jumlah=999),
    ], filename="ready2.csv")

    mock_client2 = make_mock_client(new_catalog_run2, post_results=[True])
    tracker2 = MigrationProgressTracker(progress_file)
    with patch("src.loader.pipeline.TargetAPIClient", return_value=mock_client2), \
         patch("src.loader.pipeline.MigrationProgressTracker", return_value=tracker2):
        MigrationLoadPipeline(settings).run(ready_csv2)

    df_after_run2 = MigrationProgressTracker(progress_file)._df
    row_id20 = df_after_run2[df_after_run2["new_id"] == "20"]
    assert not row_id20.empty, "Run 2 seharusnya merekam entry untuk new_id=20"
    batch2 = int(row_id20["batch_number"].iloc[0])

    assert batch2 > batch1, \
        f"Batch number harus bertambah antar run: run1={batch1}, run2={batch2}"
