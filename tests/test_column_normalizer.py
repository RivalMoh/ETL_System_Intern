"""
Unit tests untuk ColumnNormalizer (src/loader/column_normalizer.py).
"""
import json
import os
import pytest

from src.loader.column_normalizer import ColumnNormalizer


# ─── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def mapping_file(tmp_path):
    """Buat file column_mapping.json sementara untuk testing."""
    config = {
        "column_aliases": {
            "kab_kota": "kabupaten_kota",
            "kab/kota": "kabupaten_kota",
            "kabkota": "kabupaten_kota",
            "jml": "jumlah",
            "thn": "tahun",
            "tahun_data": "tahun",
            "ket": "keterangan",
            "keterangan_1": "keterangan",
            "prov": "provinsi",
            "propinsi": "provinsi",
        },
        "fuzzy_threshold": 80,
    }
    path = str(tmp_path / "column_mapping.json")
    with open(path, "w") as f:
        json.dump(config, f)
    return path


@pytest.fixture
def normalizer(mapping_file):
    """ColumnNormalizer backed oleh fixtures."""
    return ColumnNormalizer(mapping_file=mapping_file, fuzzy_threshold=80)


# ─── Exact Match (kolom sudah standar) ────────────────────────────────────────


def test_standard_columns_pass_through_unchanged(normalizer):
    """Kolom yang sudah standar (lowercase, tidak ada alias) tidak berubah."""
    record = {"kabupaten_kota": "Semarang", "jumlah": 100, "tahun": 2022}
    result = normalizer.normalize_record(record, dataset_id="test")
    assert result == record


# ─── Explicit Alias Match ────────────────────────────────────────────────────


def test_explicit_alias_kab_kota_renamed(normalizer):
    """'kab_kota' harus di-rename ke 'kabupaten_kota' via explicit alias."""
    record = {"kab_kota": "Solo", "jumlah": 50, "tahun": 2023}
    result = normalizer.normalize_record(record, dataset_id="test")

    assert "kabupaten_kota" in result
    assert "kab_kota" not in result
    assert result["kabupaten_kota"] == "Solo"


def test_explicit_alias_jml_renamed(normalizer):
    """'jml' harus di-rename ke 'jumlah' via explicit alias."""
    record = {"jml": 999, "tahun": 2022}
    result = normalizer.normalize_record(record, dataset_id="test")

    assert "jumlah" in result
    assert result["jumlah"] == 999


def test_explicit_alias_tahun_data_renamed(normalizer):
    """'tahun_data' harus di-rename ke 'tahun' via explicit alias."""
    record = {"tahun_data": 2023, "jumlah": 100}
    result = normalizer.normalize_record(record, dataset_id="test")

    assert "tahun" in result
    assert result["tahun"] == 2023


def test_explicit_alias_keterangan_1_renamed(normalizer):
    """'keterangan_1' harus di-rename ke 'keterangan' via explicit alias."""
    record = {"keterangan_1": "catatan penting", "tahun": 2022}
    result = normalizer.normalize_record(record, dataset_id="test")

    assert "keterangan" in result


def test_explicit_alias_prov_renamed(normalizer):
    """'prov' dan 'propinsi' harus di-rename ke 'provinsi'."""
    record1 = {"prov": "Jateng", "tahun": 2022}
    record2 = {"propinsi": "Jateng", "tahun": 2022}

    result1 = normalizer.normalize_record(record1, dataset_id="test1")
    result2 = normalizer.normalize_record(record2, dataset_id="test2")

    assert result1.get("provinsi") == "Jateng"
    assert result2.get("provinsi") == "Jateng"


# ─── Case Normalization ──────────────────────────────────────────────────────


def test_uppercase_columns_lowered(normalizer):
    """Kolom dengan huruf besar harus di-lowercase."""
    record = {"Kecamatan": "Gajahmungkur", "Tahun": 2022}
    result = normalizer.normalize_record(record, dataset_id="test")

    # Minimal: key harus lowercase
    keys = set(result.keys())
    for k in keys:
        assert k == k.lower(), f"Key '{k}' masih uppercase"


def test_mixed_case_alias_still_matched(normalizer):
    """'Kab_Kota' (mixed case) harus tetap jadi 'kabupaten_kota' via alias."""
    record = {"Kab_Kota": "Kudus", "tahun": 2022}
    result = normalizer.normalize_record(record, dataset_id="test")

    assert "kabupaten_kota" in result
    assert result["kabupaten_kota"] == "Kudus"


# ─── Fuzzy Match ─────────────────────────────────────────────────────────────


def test_fuzzy_match_close_column_name(tmp_path):
    """Kolom 'kabupaten_kta' (typo) harus fuzzy match ke 'kabupaten_kota' jika target columns known."""
    config = {"column_aliases": {}, "fuzzy_threshold": 80}
    path = str(tmp_path / "mapping.json")
    with open(path, "w") as f:
        json.dump(config, f)

    normalizer = ColumnNormalizer(
        mapping_file=path,
        fuzzy_threshold=80,
        target_columns=["kabupaten_kota", "jumlah", "tahun"],
    )
    record = {"kabupaten_kta": "Semarang", "tahun": 2022}
    result = normalizer.normalize_record(record, dataset_id="test")

    assert "kabupaten_kota" in result, \
        f"'kabupaten_kta' seharusnya fuzzy match ke 'kabupaten_kota', got keys: {list(result.keys())}"


# ─── Unmapped Columns ────────────────────────────────────────────────────────


def test_unmapped_columns_passed_through_as_lowercase(normalizer):
    """Kolom yang tidak ada di alias manapun tetap dikirim (lowercase)."""
    record = {"Kolom_Aneh_Sekali": "nilai", "tahun": 2022}
    result = normalizer.normalize_record(record, dataset_id="test")

    assert "kolom_aneh_sekali" in result
    assert result["kolom_aneh_sekali"] == "nilai"


# ─── Multiple Records (cache behavior) ──────────────────────────────────────


def test_same_dataset_uses_cached_mapping(normalizer):
    """Dua record dari dataset yg sama harus pakai mapping yg konsisten."""
    record1 = {"kab_kota": "Solo", "jml": 100, "tahun": 2022}
    record2 = {"kab_kota": "Kudus", "jml": 200, "tahun": 2023}

    result1 = normalizer.normalize_record(record1, dataset_id="ds1")
    result2 = normalizer.normalize_record(record2, dataset_id="ds1")

    assert "kabupaten_kota" in result1
    assert "kabupaten_kota" in result2
    assert "jumlah" in result1
    assert "jumlah" in result2


# ─── Rename Report ────────────────────────────────────────────────────────────


def test_rename_report_populated_after_normalization(normalizer):
    """get_rename_report() harus berisi entry setelah kolom di-rename."""
    record = {"kab_kota": "Solo", "jml": 100, "tahun": 2022}
    normalizer.normalize_record(record, dataset_id="ds_test")

    report = normalizer.get_rename_report()
    assert len(report) > 0

    old_cols_renamed = {r["old_column"] for r in report}
    assert "kab_kota" in old_cols_renamed
    assert "jml" in old_cols_renamed


def test_rename_report_saved_to_csv(normalizer, tmp_path):
    """save_rename_report() harus menghasilkan file CSV."""
    normalizer.normalize_record(
        {"kab_kota": "Semarang", "tahun": 2022}, dataset_id="ds1"
    )

    output_path = str(tmp_path / "report.csv")
    normalizer.save_rename_report(output_path=output_path)

    assert os.path.exists(output_path)


def test_no_rename_no_report_file(normalizer, tmp_path):
    """Jika tidak ada rename, report file tidak perlu dibuat."""
    normalizer.normalize_record(
        {"tahun": 2022, "jumlah": 100}, dataset_id="clean"
    )

    output_path = str(tmp_path / "report.csv")
    normalizer.save_rename_report(output_path=output_path)

    assert not os.path.exists(output_path)


# ─── Missing Mapping File ────────────────────────────────────────────────────


def test_missing_mapping_file_falls_back_to_fuzzy_only(tmp_path):
    """Jika file mapping tidak ada, normalizer tetap bekerja (hanya lowercase)."""
    normalizer = ColumnNormalizer(
        mapping_file=str(tmp_path / "nonexistent.json"),
        fuzzy_threshold=80,
    )

    record = {"Kecamatan": "Baru", "tahun": 2022}
    result = normalizer.normalize_record(record, dataset_id="test")

    # Tetap bekerja tanpa crash
    assert "kecamatan" in result


# ─── Values Preserved ────────────────────────────────────────────────────────


def test_values_not_modified_during_normalization(normalizer):
    """Normalisasi hanya mengubah KEY, bukan VALUE."""
    record = {"kab_kota": "Kota Semarang", "jml": 999.5, "tahun": 2022}
    result = normalizer.normalize_record(record, dataset_id="test")

    assert result["kabupaten_kota"] == "Kota Semarang"
    assert result["jumlah"] == 999.5
    assert result["tahun"] == 2022
