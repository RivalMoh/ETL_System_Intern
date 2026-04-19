import pandas as pd
import pytest

from src.data_assessor import DataAssessor


def test_assessor_tagging_logic(messy_harvest_data):
    result_df = (
        DataAssessor(messy_harvest_data)
        .flag_missing_values(required_columns=["province", "harvest_tonnage"])
        .flag_duplicates(["province", "year"])
        .mark_ready()
    )

    # Duplicate rows (0 and 1)
    assert result_df.loc[0, "migration_status"] == "flagged"
    assert "Duplicate subset" in result_df.loc[0, "flag_reason"]

    assert result_df.loc[1, "migration_status"] == "flagged"
    assert "Duplicate subset" in result_df.loc[1, "flag_reason"]

    # Clean row
    assert result_df.loc[2, "migration_status"] == "ready"
    assert result_df.loc[2, "flag_reason"] == ""

    # Missing province
    assert result_df.loc[3, "migration_status"] == "flagged"
    assert "Missing 'province'" in result_df.loc[3, "flag_reason"]


def test_missing_required_column_is_recorded_as_issue(messy_harvest_data):
    assessor = DataAssessor(messy_harvest_data).flag_missing_values(
        required_columns=["province", "unknown_col"]
    )

    assert any(
        "Missing required columns" in issue for issue in assessor.assessment_issues
    )


def test_duplicate_check_missing_subset_column_is_safe(messy_harvest_data):
    assessor = DataAssessor(messy_harvest_data).flag_duplicates(
        ["province", "not_exists"]
    )

    assert any(
        "Duplicate check skipped" in issue for issue in assessor.assessment_issues
    )


# ═══════════════════════════════════════════════════════════════════════════
# warn_suspicious_year Tests
# ═══════════════════════════════════════════════════════════════════════════


def test_year_below_range_gets_warning():
    """Tahun 1999 (di bawah min=2000) harus punya WARNING di flag_reason."""
    df = pd.DataFrame({"tahun": [1999, 2022], "jumlah": [100, 200]})
    result = (
        DataAssessor(df)
        .warn_suspicious_year(min_year=2000, max_year=2025)
        .mark_ready()
    )

    # Baris 0: tahun 1999 → WARNING
    assert "WARNING" in result.loc[0, "flag_reason"]
    assert "tahun di luar range" in result.loc[0, "flag_reason"]

    # Baris 1: tahun 2022 → tidak ada warning
    assert result.loc[1, "flag_reason"] == ""


def test_year_above_range_gets_warning():
    """Tahun 2030 (di atas max=2025) harus punya WARNING di flag_reason."""
    df = pd.DataFrame({"tahun": [2022, 2030], "jumlah": [100, 200]})
    result = (
        DataAssessor(df)
        .warn_suspicious_year(min_year=2000, max_year=2025)
        .mark_ready()
    )

    assert "WARNING" in result.loc[1, "flag_reason"]
    assert result.loc[0, "flag_reason"] == ""


def test_year_in_range_no_warning():
    """Semua tahun valid (2000-2025) → tidak ada warning."""
    df = pd.DataFrame({"tahun": [2000, 2010, 2025], "jumlah": [1, 2, 3]})
    result = (
        DataAssessor(df)
        .warn_suspicious_year(min_year=2000, max_year=2025)
        .mark_ready()
    )

    for idx in range(3):
        assert result.loc[idx, "flag_reason"] == ""


def test_year_warning_does_not_block_row():
    """
    WARNING TIDAK boleh mengubah migration_status.
    Baris dengan warning harus tetap 'ready', BUKAN 'flagged'.
    """
    df = pd.DataFrame({"tahun": [1, 99, 3000], "jumlah": [1, 2, 3]})
    result = (
        DataAssessor(df)
        .warn_suspicious_year(min_year=2000, max_year=2025)
        .mark_ready()
    )

    # Semua baris harus tetap ready (bukan flagged)
    for idx in range(3):
        assert result.loc[idx, "migration_status"] == "ready", \
            f"Baris {idx}: seharusnya 'ready' bukan '{result.loc[idx, 'migration_status']}'"
        assert "WARNING" in result.loc[idx, "flag_reason"]


def test_year_warning_missing_column_safe():
    """Jika kolom 'tahun' tidak ada, warn_suspicious_year tidak crash."""
    df = pd.DataFrame({"kota": ["Solo"], "jumlah": [100]})
    result = (
        DataAssessor(df)
        .warn_suspicious_year(min_year=2000, max_year=2025)
        .mark_ready()
    )
    # Tidak crash, baris tetap ready
    assert result.loc[0, "migration_status"] == "ready"
