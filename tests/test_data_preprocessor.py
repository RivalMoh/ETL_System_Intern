"""
Unit tests untuk DataPreprocessor (src/data_preprocessor.py).
"""
import json
import os
import pytest
import pandas as pd

from src.data_preprocessor import DataPreprocessor


# ─── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def mapping_file(tmp_path):
    """Column mapping file sementara untuk testing."""
    config = {
        "column_aliases": {
            "kab_kota": "nama_wilayah",
            "kab/kota": "nama_wilayah",
            "kabkota": "nama_wilayah",
            "kabupaten": "nama_wilayah",
            "kod_wil": "kode_wilayah",
            "thn": "tahun",
            "tahun_data": "tahun",
            "year": "tahun",
            "jml": "jumlah",
            "prov": "provinsi",
        },
        "fuzzy_threshold": 80,
    }
    path = str(tmp_path / "column_mapping.json")
    with open(path, "w") as f:
        json.dump(config, f)
    return path


# ═══════════════════════════════════════════════════════════════════════════
# Column Normalization Tests
# ═══════════════════════════════════════════════════════════════════════════


def test_explicit_alias_rename(mapping_file):
    """'kab_kota' harus di-rename ke 'nama_wilayah' via explicit alias."""
    df = pd.DataFrame({"kab_kota": ["Solo"], "tahun": [2022]})
    result = DataPreprocessor(df, mapping_file).normalize_columns().get_result()

    assert "nama_wilayah" in result.columns
    assert "kab_kota" not in result.columns
    assert result["nama_wilayah"].iloc[0] == "Solo"


def test_explicit_alias_kod_wil_to_kode_wilayah(mapping_file):
    """'kod_wil' harus di-rename ke 'kode_wilayah'."""
    df = pd.DataFrame({"kod_wil": ["33.20"], "tahun": [2022]})
    result = DataPreprocessor(df, mapping_file).normalize_columns().get_result()

    assert "kode_wilayah" in result.columns
    assert "kod_wil" not in result.columns


def test_explicit_alias_case_insensitive(mapping_file):
    """'Kab_Kota' (mixed case) → 'nama_wilayah' via alias lookup (lowercased)."""
    df = pd.DataFrame({"Kab_Kota": ["Kudus"], "tahun": [2022]})
    result = DataPreprocessor(df, mapping_file).normalize_columns().get_result()

    assert "nama_wilayah" in result.columns


def test_column_already_standard_not_changed(mapping_file):
    """Kolom yang sudah standar (misal 'kode_wilayah') tidak berubah."""
    df = pd.DataFrame({"kode_wilayah": ["33.20"], "tahun": [2022]})
    result = DataPreprocessor(df, mapping_file).normalize_columns().get_result()

    assert "kode_wilayah" in result.columns


def test_tahun_fallback_when_no_tahun_column(mapping_file):
    """Jika kolom 'tahun' belum ada, 'thn' di-rename via alias → 'tahun'."""
    df = pd.DataFrame({"thn": [2022], "jumlah": [100]})
    result = DataPreprocessor(df, mapping_file).normalize_columns().get_result()

    assert "tahun" in result.columns
    assert "thn" not in result.columns


def test_uppercase_columns_lowered(mapping_file):
    """Kolom uppercase yang bukan alias → jadi lowercase."""
    df = pd.DataFrame({"Kecamatan": ["Gajahmungkur"], "tahun": [2022]})
    result = DataPreprocessor(df, mapping_file).normalize_columns().get_result()

    assert "kecamatan" in result.columns
    assert "Kecamatan" not in result.columns


def test_missing_mapping_file_still_works(tmp_path):
    """Tanpa file mapping, preprocessor tetap berjalan (hanya lowercase)."""
    df = pd.DataFrame({"Kecamatan": ["Solo"], "tahun": [2022]})
    nonexistent = str(tmp_path / "nope.json")
    result = DataPreprocessor(df, nonexistent).normalize_columns().get_result()

    assert "kecamatan" in result.columns


# ═══════════════════════════════════════════════════════════════════════════
# Whitespace Normalization Tests
# ═══════════════════════════════════════════════════════════════════════════


def test_strip_leading_trailing_whitespace(mapping_file):
    """Strip spasi di awal dan akhir string."""
    df = pd.DataFrame({"kota": ["  Semarang  "], "tahun": [2022]})
    result = DataPreprocessor(df, mapping_file).strip_whitespace().get_result()

    assert result["kota"].iloc[0] == "Semarang"


def test_collapse_multiple_spaces(mapping_file):
    """Multiple spaces di tengah string di-collapse jadi 1."""
    df = pd.DataFrame({"kota": ["Jawa   Tengah"], "tahun": [2022]})
    result = DataPreprocessor(df, mapping_file).strip_whitespace().get_result()

    assert result["kota"].iloc[0] == "Jawa Tengah"


def test_mixed_whitespace_issues(mapping_file):
    """Kombinasi: leading + trailing + multi-space sekaligus."""
    df = pd.DataFrame({"kota": ["  Kota   Semarang  "], "tahun": [2022]})
    result = DataPreprocessor(df, mapping_file).strip_whitespace().get_result()

    assert result["kota"].iloc[0] == "Kota Semarang"


def test_whitespace_preserves_nan(mapping_file):
    """NaN tetap NaN setelah whitespace strip, bukan menjadi string 'nan'."""
    df = pd.DataFrame({"kota": [None, "Solo"], "tahun": [2022, 2023]})
    result = DataPreprocessor(df, mapping_file).strip_whitespace().get_result()

    assert result["kota"].iloc[0] is None
    assert result["kota"].iloc[1] == "Solo"


def test_whitespace_numeric_columns_unaffected(mapping_file):
    """Kolom numerik (int, float) tidak terpengaruh oleh whitespace strip."""
    df = pd.DataFrame({"tahun": [2022], "jumlah": [100.5]})
    result = DataPreprocessor(df, mapping_file).strip_whitespace().get_result()

    assert result["tahun"].iloc[0] == 2022
    assert result["jumlah"].iloc[0] == 100.5


# ═══════════════════════════════════════════════════════════════════════════
# Kode Wilayah Fix Tests
# ═══════════════════════════════════════════════════════════════════════════


def test_kode_wilayah_4digit_fixed(mapping_file):
    """'3320' → '33.20' (kab/kota)."""
    df = pd.DataFrame({"kode_wilayah": ["3320"], "tahun": [2022]})
    result = DataPreprocessor(df, mapping_file).fix_kode_wilayah().get_result()

    assert result["kode_wilayah"].iloc[0] == "33.20"


def test_kode_wilayah_6digit_fixed(mapping_file):
    """'332001' → '33.20.01' (kecamatan)."""
    df = pd.DataFrame({"kode_wilayah": ["332001"], "tahun": [2022]})
    result = DataPreprocessor(df, mapping_file).fix_kode_wilayah().get_result()

    assert result["kode_wilayah"].iloc[0] == "33.20.01"


def test_kode_wilayah_8digit_fixed(mapping_file):
    """'33200107' → '33.20.01.07' (kelurahan)."""
    df = pd.DataFrame({"kode_wilayah": ["33200107"], "tahun": [2022]})
    result = DataPreprocessor(df, mapping_file).fix_kode_wilayah().get_result()

    assert result["kode_wilayah"].iloc[0] == "33.20.01.07"


def test_kode_wilayah_10digit_fixed(mapping_file):
    """'3320010007' → '33.20.01.0007' (kelurahan panjang)."""
    df = pd.DataFrame({"kode_wilayah": ["3320010007"], "tahun": [2022]})
    result = DataPreprocessor(df, mapping_file).fix_kode_wilayah().get_result()

    assert result["kode_wilayah"].iloc[0] == "33.20.01.0007"


def test_kode_wilayah_already_correct_not_changed(mapping_file):
    """'33.20' sudah benar → tidak berubah."""
    df = pd.DataFrame({"kode_wilayah": ["33.20"], "tahun": [2022]})
    result = DataPreprocessor(df, mapping_file).fix_kode_wilayah().get_result()

    assert result["kode_wilayah"].iloc[0] == "33.20"


def test_kode_wilayah_non_numeric_skipped(mapping_file):
    """'abc' → tetap 'abc' (non-numeric, dilewati)."""
    df = pd.DataFrame({"kode_wilayah": ["abc"], "tahun": [2022]})
    result = DataPreprocessor(df, mapping_file).fix_kode_wilayah().get_result()

    assert result["kode_wilayah"].iloc[0] == "abc"


def test_kode_wilayah_nan_preserved(mapping_file):
    """NaN → tetap NaN."""
    df = pd.DataFrame({"kode_wilayah": [None], "tahun": [2022]})
    result = DataPreprocessor(df, mapping_file).fix_kode_wilayah().get_result()

    assert pd.isna(result["kode_wilayah"].iloc[0])


def test_kode_wilayah_too_short_skipped(mapping_file):
    """'33' (hanya 2 digit) → tidak bisa diformat, tetap '33'."""
    df = pd.DataFrame({"kode_wilayah": ["33"], "tahun": [2022]})
    result = DataPreprocessor(df, mapping_file).fix_kode_wilayah().get_result()

    assert result["kode_wilayah"].iloc[0] == "33"


def test_kode_wilayah_column_missing_no_error(mapping_file):
    """Jika kolom kode_wilayah tidak ada → skip tanpa error."""
    df = pd.DataFrame({"kota": ["Solo"], "tahun": [2022]})
    result = DataPreprocessor(df, mapping_file).fix_kode_wilayah().get_result()

    assert "kota" in result.columns  # no crash


# ═══════════════════════════════════════════════════════════════════════════
# Chaining & Integration Tests
# ═══════════════════════════════════════════════════════════════════════════


def test_full_chain_all_steps(mapping_file):
    """Semua step di-chain: rename + whitespace + kode_wilayah."""
    df = pd.DataFrame({
        "Kab_Kota": ["  Kota   Semarang  "],
        "kod_wil": ["3320"],
        "thn": [2022],
        "jumlah": [100],
    })

    result = (
        DataPreprocessor(df, mapping_file)
        .normalize_columns()
        .strip_whitespace()
        .fix_kode_wilayah()
        .get_result()
    )

    # Column rename
    assert "nama_wilayah" in result.columns
    assert "kode_wilayah" in result.columns
    assert "tahun" in result.columns

    # Whitespace cleaned
    assert result["nama_wilayah"].iloc[0] == "Kota Semarang"

    # kode_wilayah fixed
    assert result["kode_wilayah"].iloc[0] == "33.20"


def test_changes_log_populated(mapping_file):
    """get_changes_log() harus berisi entry setelah preprocessing."""
    df = pd.DataFrame({"kab_kota": ["Solo"], "tahun": [2022]})
    preprocessor = DataPreprocessor(df, mapping_file)
    preprocessor.normalize_columns()

    log = preprocessor.get_changes_log()
    assert len(log) > 0
    assert any(entry["action"] == "column_rename" for entry in log)


def test_original_df_not_mutated(mapping_file):
    """DataPreprocessor TIDAK boleh mutasi DataFrame asli."""
    df_original = pd.DataFrame({"kab_kota": ["Solo"], "tahun": [2022]})
    original_columns = list(df_original.columns)

    DataPreprocessor(df_original, mapping_file).normalize_columns().get_result()

    # Original DataFrame harus tetap sama
    assert list(df_original.columns) == original_columns
