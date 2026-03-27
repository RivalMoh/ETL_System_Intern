import pandas as pd
import pytest

from src.load import LoadGate


def test_select_rows_default_only_ready_status():
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "migration_status": ["ready", "flagged", "pending"],
        }
    )

    result = LoadGate().select_rows(df)

    assert len(result) == 1
    assert result.iloc[0]["id"] == 1


def test_select_rows_with_custom_allowed_statuses():
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "migration_status": ["ready", "flagged", "pending"],
        }
    )

    result = LoadGate(allowed_statuses=["ready", "flagged"]).select_rows(df)

    assert len(result) == 2
    assert set(result["id"].tolist()) == {1, 2}


def test_select_rows_without_migration_status_raises():
    df = pd.DataFrame({"id": [1, 2]})

    with pytest.raises(ValueError, match="migration_status"):
        LoadGate().select_rows(df)


def test_build_summary_counts_loadable_and_blocked_rows():
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "migration_status": ["ready", "flagged", "ready", "pending"],
        }
    )

    summary = LoadGate().build_summary(df)

    assert summary["total_rows"] == 4
    assert summary["loadable_rows"] == 2
    assert summary["blocked_rows"] == 2


def test_load_gate_requires_non_empty_allowed_statuses():
    with pytest.raises(ValueError, match="allowed_statuses"):
        LoadGate(allowed_statuses=[])
