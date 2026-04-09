"""
System Tests — Audit Pipeline (src/pipeline.py)

Menguji MigrationPipeline.run() secara end-to-end dengan menggunakan
fake data dan mock API calls, tanpa koneksi ke server nyata.

Setiap test memverifikasi HASIL AKHIR pipeline (file output yang dihasilkan,
data di report), bukan detail implementasi internal.
"""
import json
import os
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

from src.pipeline import MigrationPipeline
from src.config import AppSettings


# ─── Fixtures ────────────────────────────────────────────────────────────────


def make_settings(**overrides) -> AppSettings:
    """AppSettings dengan nilai minimal untuk testing (tidak perlu .env)."""
    settings = AppSettings.__new__(AppSettings)
    settings.base_url = "https://api.test/v1"
    settings.api_key = "test-key"
    settings.max_pages = 1
    settings.max_datasets = 10
    settings.dup_threshold = 85
    settings.dup_sample_size = 3
    settings.require_columns = ["tahun", "jumlah"]
    settings.allowed_load_statuses = ["ready"]
    settings.new_base_url = "https://target.test/v1"
    settings.new_api_key = "target-key"
    for k, v in overrides.items():
        setattr(settings, k, v)
    return settings


def make_catalog_df(*entries) -> pd.DataFrame:
    """[{"id": "1", "judul": "..."}, ...] → DataFrame katalog."""
    return pd.DataFrame(entries)


def make_detail_df(dataset_id: str, rows: list) -> pd.DataFrame:
    """Buat df_detail dengan kolom tahun + data kolom + dataset_id."""
    df = pd.DataFrame(rows)
    df["dataset_id"] = dataset_id
    return df


@pytest.fixture
def settings():
    return make_settings()


def make_pipeline_with_fake_api(settings, catalog_df, detail_map: dict):
    """
    Buat MigrationPipeline dengan APIExtractor yang di-mock.
    detail_map: {dataset_id: pd.DataFrame}
    """
    pipeline = MigrationPipeline.__new__(MigrationPipeline)
    pipeline.settings = settings

    # Mock extractor
    fake_extractor = MagicMock()
    fake_extractor.get_dataset_catalog.return_value = catalog_df
    fake_extractor.get_dataset_details.side_effect = lambda did: detail_map.get(
        str(did), pd.DataFrame()
    )

    pipeline.extractor = fake_extractor

    from src.load import LoadGate
    from src.reporting import ReportGenerator
    pipeline.load_gate = LoadGate(allowed_statuses=["ready"])
    pipeline.reporter = MagicMock()  # tidak perlu generate file nyata
    return pipeline


# ─── Scenario 1: Happy Path (semua data bersih) ──────────────────────────────


def test_audit_happy_path_routes_clean_data_to_ready(settings):
    """
    Scenario: 2 dataset bersih, tidak suspect, tidak ada missing values.
    Expected: semua baris masuk ke load_ready_rows, tidak ada di manager_review_rows.
    """
    catalog = make_catalog_df(
        {"id": "1", "judul": "Data Padi Semarang"},
        {"id": "2", "judul": "Data Beras Solo"},
    )
    detail_1 = make_detail_df("1", [
        {"tahun": 2022, "jumlah": 100},
        {"tahun": 2023, "jumlah": 150},
    ])
    detail_2 = make_detail_df("2", [
        {"tahun": 2022, "jumlah": 200},
    ])

    pipeline = make_pipeline_with_fake_api(settings, catalog, {"1": detail_1, "2": detail_2})

    captured_ready = []
    captured_review = []

    original_pack = pipeline._pack_and_route_data

    def capture_pack(df_assessed, dataset_id, title, is_suspect, load_decision, ready_list, review_list):
        original_pack(df_assessed, dataset_id, title, is_suspect, load_decision, ready_list, review_list)
        captured_ready.extend(ready_list[-1:])
        captured_review.extend(review_list[-len(review_list):])

    with patch.object(pipeline, "_pack_and_route_data", side_effect=capture_pack):
        # Langsung panggil run() — reporter di-mock jadi tidak nulis file
        pipeline.run()

    # Verifikasi reporter dipanggil dengan data yang benar
    assert pipeline.reporter.generate_hybrid_report.called
    call_args = pipeline.reporter.generate_hybrid_report.call_args[0]
    load_ready_rows = call_args[5]
    manager_review_rows = call_args[6]

    # Kedua dataset clean → semua ke load_ready
    assert len(load_ready_rows) == 2
    assert len(manager_review_rows) == 0


def test_audit_happy_path_row_data_json_is_valid_json(settings):
    """
    Setiap baris di load_ready_rows harus punya field Row_Data_JSON yang valid JSON.
    """
    catalog = make_catalog_df({"id": "1", "judul": "Data Padi"})
    detail = make_detail_df("1", [{"tahun": 2022, "jumlah": 500, "kabupaten": "Solo"}])

    pipeline = make_pipeline_with_fake_api(settings, catalog, {"1": detail})
    pipeline.run()

    call_args = pipeline.reporter.generate_hybrid_report.call_args[0]
    load_ready_rows = call_args[5]

    assert len(load_ready_rows) == 1
    df_ready = load_ready_rows[0]

    # Setiap Row_Data_JSON harus bisa di-parse sebagai JSON
    for val in df_ready["Row_Data_JSON"]:
        parsed = json.loads(val)
        assert isinstance(parsed, dict)


# ─── Scenario 2: Suspect Dataset (judul mirip) ───────────────────────────────


def test_audit_suspect_dataset_all_rows_go_to_review(settings):
    """
    Scenario: 2 dataset dengan judul hampir identik → suspect.
    Expected: SEMUA baris (ready dan flagged) harus masuk ke manager_review_rows.
    Tidak ada baris dari dataset suspect yang langsung ke load_ready.
    """
    catalog = make_catalog_df(
        {"id": "A", "judul": "Data Padi Jawa Tengah"},
        {"id": "B", "judul": "Jawa Tengah Data Padi"},  # sangat mirip
    )
    # Data identik → akan terdeteksi duplikat oleh fingerprint
    detail_a = make_detail_df("A", [
        {"tahun": 2022, "jumlah": 100},
        {"tahun": 2023, "jumlah": 150},
    ])
    detail_b = make_detail_df("B", [
        {"tahun": 2022, "jumlah": 100},
        {"tahun": 2023, "jumlah": 150},
    ])

    pipeline = make_pipeline_with_fake_api(settings, catalog, {"A": detail_a, "B": detail_b})
    # Override settings agar kedua dataset diproses
    pipeline.settings.max_datasets = 2

    pipeline.run()

    call_args = pipeline.reporter.generate_hybrid_report.call_args[0]
    load_ready_rows = call_args[5]
    manager_review_rows = call_args[6]

    # Tidak ada yang boleh masuk load_ready karena keduanya suspect
    assert len(load_ready_rows) == 0, \
        f"Dataset suspect seharusnya tidak masuk load_ready, tapi ada {len(load_ready_rows)}"
    assert len(manager_review_rows) > 0


def test_audit_suspect_dataset_catalog_suspect_flag_is_set(settings):
    """
    Setiap baris di manager_review yang berasal dari suspect dataset
    harus punya Catalog_Suspect = True.
    """
    catalog = make_catalog_df(
        {"id": "A", "judul": "Data Padi Jawa Tengah"},
        {"id": "B", "judul": "Jawa Tengah Data Padi"},
    )
    detail_a = make_detail_df("A", [{"tahun": 2022, "jumlah": 100}])
    detail_b = make_detail_df("B", [{"tahun": 2022, "jumlah": 100}])

    pipeline = make_pipeline_with_fake_api(settings, catalog, {"A": detail_a, "B": detail_b})
    pipeline.settings.max_datasets = 2
    pipeline.run()

    call_args = pipeline.reporter.generate_hybrid_report.call_args[0]
    manager_review_rows = call_args[6]

    for chunk in manager_review_rows:
        assert chunk["Catalog_Suspect"].all(), \
            "Setiap baris dari dataset suspect harus punya Catalog_Suspect=True"


# ─── Scenario 3: Flagged Data (missing required columns) ─────────────────────


def test_audit_flagged_rows_go_to_review(settings):
    """
    Scenario: Dataset dengan beberapa baris yang hilang kolom 'jumlah' (required).
    Expected: Dataset masuk ke manager_review karena ada flagged rows.
    """
    catalog = make_catalog_df({"id": "1", "judul": "Data Unik"})
    # Baris 0 & 1: normal | Baris 2: missing kolom 'jumlah'
    detail = make_detail_df("1", [
        {"tahun": 2022, "jumlah": 100},
        {"tahun": 2023, "jumlah": 200},
        {"tahun": 2024, "jumlah": None},  # ← missing → akan di-flag
    ])

    pipeline = make_pipeline_with_fake_api(settings, catalog, {"1": detail})
    pipeline.run()

    call_args = pipeline.reporter.generate_hybrid_report.call_args[0]
    load_ready_rows = call_args[5]
    manager_review_rows = call_args[6]

    assert len(load_ready_rows) == 0
    assert len(manager_review_rows) == 1


def test_audit_flagged_dataset_review_contains_all_rows(settings):
    """
    Review rows harus berisi SEMUA baris dataset (ready + flagged),
    bukan hanya yang flagged saja.
    """
    catalog = make_catalog_df({"id": "1", "judul": "Data Campuran"})
    detail = make_detail_df("1", [
        {"tahun": 2022, "jumlah": 100},   # ready
        {"tahun": 2023, "jumlah": 200},   # ready
        {"tahun": 2024, "jumlah": None},  # flagged
    ])

    pipeline = make_pipeline_with_fake_api(settings, catalog, {"1": detail})
    pipeline.run()

    call_args = pipeline.reporter.generate_hybrid_report.call_args[0]
    manager_review_rows = call_args[6]

    total_rows_in_review = sum(len(chunk) for chunk in manager_review_rows)
    assert total_rows_in_review == 3, \
        "Semua 3 baris harus ada di review (bukan hanya yang flagged)"


# ─── Scenario 4: Empty & Error Handling ──────────────────────────────────────


def test_audit_empty_catalog_exits_gracefully(settings):
    """
    Jika katalog API kosong, pipeline harus berhenti tanpa crash
    dan reporter tidak dipanggil.
    """
    catalog = pd.DataFrame()  # kosong

    pipeline = make_pipeline_with_fake_api(settings, catalog, {})
    pipeline.run()

    pipeline.reporter.generate_hybrid_report.assert_not_called()


def test_audit_api_error_on_detail_records_failure_in_summary(settings):
    """
    Jika get_dataset_details() raise exception untuk satu dataset,
    dataset itu harus masuk ke summary dengan Load_Decision='skipped'.
    Pipeline tidak boleh crash.
    """
    catalog = make_catalog_df(
        {"id": "1", "judul": "Data OK"},
        {"id": "2", "judul": "Data Gagal"},
    )
    detail_ok = make_detail_df("1", [{"tahun": 2022, "jumlah": 100}])

    pipeline = make_pipeline_with_fake_api(settings, catalog, {"1": detail_ok})
    # Dataset "2" tidak ada di detail_map → extractor return DataFrame kosong
    # Simulasikan error dengan override extractor
    pipeline.extractor.get_dataset_details.side_effect = lambda did: (
        detail_ok if str(did) == "1" else (_ for _ in ()).throw(RuntimeError("API Timeout"))
    )

    pipeline.run()  # tidak boleh raise

    call_args = pipeline.reporter.generate_hybrid_report.call_args[0]
    df_micro = call_args[3]

    # Dataset "2" harus kelihatan di summary sebagai skipped
    skipped = df_micro[df_micro["Load_Decision"] == "skipped"]
    assert len(skipped) == 1
    assert skipped.iloc[0]["Dataset_Id"] == "2"


def test_audit_empty_detail_records_failure_in_summary(settings):
    """
    Dataset dengan detail kosong (0 baris) harus masuk ke summary
    dengan Load_Decision='skipped', bukan menyebabkan crash.
    """
    catalog = make_catalog_df({"id": "1", "judul": "Tabel Kosong"})
    pipeline = make_pipeline_with_fake_api(settings, catalog, {"1": pd.DataFrame()})
    pipeline.run()

    call_args = pipeline.reporter.generate_hybrid_report.call_args[0]
    df_micro = call_args[3]

    assert df_micro.iloc[0]["Load_Decision"] == "skipped"


# ─── Scenario 5: Reporter dipanggil dengan argumen yang benar ───────────────


def test_audit_reporter_receives_correct_argument_count(settings):
    """
    generate_hybrid_report harus dipanggil dengan tepat 7 argumen positional:
    df_catalog, df_duplicates, df_skipped, df_micro, df_load_summary,
    load_ready_rows, manager_review_rows.
    """
    catalog = make_catalog_df({"id": "1", "judul": "Data Padi"})
    detail = make_detail_df("1", [{"tahun": 2022, "jumlah": 50}])

    pipeline = make_pipeline_with_fake_api(settings, catalog, {"1": detail})
    pipeline.run()

    assert pipeline.reporter.generate_hybrid_report.called
    call_args = pipeline.reporter.generate_hybrid_report.call_args[0]
    assert len(call_args) == 7, \
        f"Reporter harus menerima 7 argumen, tapi menerima {len(call_args)}"


def test_audit_micro_summary_contains_correct_columns(settings):
    """
    df_micro_summary (argumen ke-4 reporter) harus punya kolom wajib.
    """
    catalog = make_catalog_df({"id": "1", "judul": "Data Padi"})
    detail = make_detail_df("1", [{"tahun": 2022, "jumlah": 100}])

    pipeline = make_pipeline_with_fake_api(settings, catalog, {"1": detail})
    pipeline.run()

    call_args = pipeline.reporter.generate_hybrid_report.call_args[0]
    df_micro = call_args[3]

    required_cols = {"Dataset_Id", "Judul_Tabel", "Total_Rows",
                     "Baris_Siap_Load", "Load_Decision"}
    missing = required_cols - set(df_micro.columns)
    assert not missing, f"Kolom wajib tidak ada di df_micro: {missing}"
