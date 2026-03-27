import pandas as pd
import pytest

from src.data_assessor import DataAssessor


@pytest.fixture
def messy_harvest_data():
    return pd.DataFrame(
        {
            "record_id": ["A1", "A2", "A3", "A4"],
            "province": ["Jawa Barat", "Jawa Barat", "Jawa Tengah", None],
            "year": [2023, 2023, 2024, 2025],
            "harvest_tonnage": [100.5, 100.5, 150.0, 200.0],
        }
    )


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
