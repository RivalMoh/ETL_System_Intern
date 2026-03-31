import pytest
import pandas as pd

from src.loader.progress_tracker import (
    MigrationProgressTracker,
    STATUS_DONE,
    STATUS_PARTIAL,
    STATUS_FAILED,
)


# Fixtures


@pytest.fixture
def tracker(tmp_path):
    """Fresh tracker backed by a temp file — isolated per test."""
    return MigrationProgressTracker(
        progress_file=str(tmp_path / "migration_progress.csv")
    )


@pytest.fixture
def populated_tracker(tracker):
    """Tracker with pre-recorded entries for query tests."""
    tracker.record("1", "Tabel Padi",       "old_1", STATUS_DONE,    rows_sent=80,  batch_number=1)
    tracker.record("2", "Tabel Jagung",     "old_2", STATUS_FAILED,  rows_sent=0,   batch_number=1)
    tracker.record("3", "Tabel Kedelai",    "old_3", STATUS_DONE,    rows_sent=50,  batch_number=1)
    tracker.record("4", "Tabel Kemiskinan", "old_4", STATUS_PARTIAL, rows_sent=20,  batch_number=1)
    return tracker


# Cold Start


def test_cold_start_returns_empty_done_ids(tracker):
    assert tracker.get_done_ids() == set()


def test_cold_start_batch_number_is_one(tracker):
    assert tracker.get_next_batch_number() == 1


def test_cold_start_summary_all_zeros(tracker):
    summary = tracker.get_summary()
    assert summary["done"] == 0
    assert summary["failed"] == 0
    assert summary["partial"] == 0
    assert summary["total_recorded"] == 0


# record()


def test_record_done_appears_in_done_ids(tracker):
    tracker.record("10", "Tabel A", "old_A", STATUS_DONE, rows_sent=100, batch_number=1)
    assert "10" in tracker.get_done_ids()


def test_record_failed_not_in_done_ids(tracker):
    tracker.record("10", "Tabel A", "old_A", STATUS_FAILED, rows_sent=0, batch_number=1)
    assert "10" not in tracker.get_done_ids()


def test_record_partial_not_in_done_ids(tracker):
    tracker.record("10", "Tabel A", "old_A", STATUS_PARTIAL, rows_sent=30, batch_number=1)
    assert "10" not in tracker.get_done_ids()


def test_record_upsert_updates_status_not_duplicate(tracker):
    """Merekam ID yang sama dua kali harus UPDATE, bukan menambah baris baru."""
    tracker.record("5", "Tabel X", "old_5", STATUS_FAILED,  rows_sent=0,  batch_number=1)
    tracker.record("5", "Tabel X", "old_5", STATUS_DONE,    rows_sent=60, batch_number=2)

    assert "5" in tracker.get_done_ids()
    assert len(tracker._df) == 1              # hanya 1 baris, bukan 2
    assert tracker._df.loc[0, "rows_sent"] == 60
    assert tracker._df.loc[0, "batch_number"] == 2


# get_summary()


def test_summary_counts_all_statuses(populated_tracker):
    summary = populated_tracker.get_summary()
    assert summary["done"] == 2
    assert summary["failed"] == 1
    assert summary["partial"] == 1
    assert summary["total_recorded"] == 4


# get_next_batch_number()

def test_batch_number_increments_from_max(populated_tracker):
    # Setelah populated_tracker selesai, max batch = 1 → next = 2
    assert populated_tracker.get_next_batch_number() == 2


def test_batch_number_increments_correctly_after_second_batch(tracker):
    tracker.record("1", "A", "o1", STATUS_DONE, batch_number=3)
    assert tracker.get_next_batch_number() == 4


# Catalog Filtering


def test_remaining_catalog_excludes_done_ids(populated_tracker):
    """Katalog yang tersisa hanya berisi tabel yang belum 'done'."""
    new_catalog = [
        {"id": "1", "judul": "Tabel Padi"},        # done → exclude
        {"id": "2", "judul": "Tabel Jagung"},       # failed → include
        {"id": "3", "judul": "Tabel Kedelai"},      # done → exclude
        {"id": "4", "judul": "Tabel Kemiskinan"},   # partial → include
        {"id": "5", "judul": "Tabel Baru"},         # new → include
    ]
    done_ids = populated_tracker.get_done_ids()
    remaining = [c for c in new_catalog if str(c.get("id")) not in done_ids]

    remaining_ids = {str(c["id"]) for c in remaining}
    assert remaining_ids == {"2", "4", "5"}
    assert "1" not in remaining_ids
    assert "3" not in remaining_ids


# Persistence


def test_progress_persists_across_instances(tmp_path):
    """State harus tetap ada saat membuat instance baru dari file yang sama."""
    progress_file = str(tmp_path / "migration_progress.csv")

    tracker1 = MigrationProgressTracker(progress_file=progress_file)
    tracker1.record("99", "Tabel Persist", "old_99", STATUS_DONE, rows_sent=42, batch_number=1)

    tracker2 = MigrationProgressTracker(progress_file=progress_file)
    assert "99" in tracker2.get_done_ids()
    assert tracker2.get_next_batch_number() == 2


def test_backward_compat_missing_column(tmp_path):
    """File progress lama yang tidak punya semua kolom tetap bisa dimuat."""
    progress_file = str(tmp_path / "old_progress.csv")

    # Simulasi file lama tanpa kolom batch_number
    old_df = pd.DataFrame([
        {"new_id": "7", "new_title": "Lama", "old_id": "o7",
         "status": STATUS_DONE, "rows_sent": 10, "migrated_at": "2025-01-01"}
    ])
    old_df.to_csv(progress_file, index=False)

    tracker = MigrationProgressTracker(progress_file=progress_file)
    assert "7" in tracker.get_done_ids()           # tetap bisa baca status done
    assert tracker.get_next_batch_number() == 1    # batch_number None → fallback ke 1
