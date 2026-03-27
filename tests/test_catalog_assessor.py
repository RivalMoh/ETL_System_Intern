import pandas as pd
import pytest

from src.catalog_assessor import CatalogAssessor
from tests.conftest import DummyExtractor


def test_group_by_title_similarity_builds_groups(sample_catalog):
    assessor = CatalogAssessor(sample_catalog, DummyExtractor())
    assessor.group_by_title_similarity(threshold=85)

    assert len(assessor.suspect_groups) >= 1
    all_group_ids = [set(g["dataset_ids"]) for g in assessor.suspect_groups]
    assert {"A", "B"} in all_group_ids


def test_group_by_title_similarity_resets_state(sample_catalog):
    assessor = CatalogAssessor(sample_catalog, DummyExtractor())

    assessor.group_by_title_similarity(threshold=85)
    first_count = len(assessor.suspect_groups)

    assessor.group_by_title_similarity(threshold=85)
    second_count = len(assessor.suspect_groups)

    assert first_count == second_count


def test_verify_with_data_sample_pairwise_duplicates():
    catalog = pd.DataFrame(
        {
            "id": ["A", "B", "C", "D"],
            "judul": [
                "Data Padi",
                "Padi Data",
                "Data Padi Kabupaten",
                "Padi Kabupaten",
            ],
        }
    )

    df_same_1 = pd.DataFrame({"col1": [1, 2], "col2": ["x", "y"]})
    df_same_2 = pd.DataFrame({"col1": [1, 2], "col2": ["x", "y"]})
    df_same_3 = pd.DataFrame({"col1": ["x", 1], "col2": [2, "y"]})
    df_diff = pd.DataFrame({"col1": [9, 10], "col2": ["m", "n"]})

    extractor = DummyExtractor(
        mapping={
            "A": df_same_1.assign(dataset_id="A"),
            "B": df_same_2.assign(dataset_id="B"),
            "C": df_diff.assign(dataset_id="C"),
            "D": df_same_3.assign(dataset_id="D"),
        }
    )

    assessor = CatalogAssessor(catalog, extractor)
    assessor.suspect_groups = [
        {"base_title": "data padi", "dataset_ids": ["A", "B", "C", "D"]}
    ]

    df_dup = assessor.verify_with_data_sample(sample_size=5)

    assert len(df_dup) == 3
    pair = {df_dup.loc[0, "ID_Tabel_A"], df_dup.loc[0, "ID_Tabel_B"]}
    assert pair == {"A", "B"}
    pair_2 = {df_dup.loc[1, "ID_Tabel_A"], df_dup.loc[1, "ID_Tabel_B"]}
    assert pair_2 == {"A", "D"} or pair_2 == {"B", "D"}


def test_verify_with_data_sample_records_skipped_rows():
    catalog = pd.DataFrame({"id": ["A", "B"], "judul": ["Data Air", "Air Data"]})

    extractor = DummyExtractor(
        mapping={"A": pd.DataFrame()},  # empty_detail
        errors={"B": RuntimeError("API down")},  # fetch_error
    )

    assessor = CatalogAssessor(catalog, extractor)
    assessor.suspect_groups = [{"base_title": "data air", "dataset_ids": ["A", "B"]}]

    df_dup = assessor.verify_with_data_sample(sample_size=5)
    df_skipped = assessor.get_skipped_rows()

    assert df_dup.empty
    assert len(df_skipped) >= 2
    reasons = set(df_skipped["Kategori_Error"].tolist())
    assert any("Empty Detail" in r for r in reasons)
    assert any("Fetch Error" in r for r in reasons)


def test_validate_required_columns():
    bad_catalog = pd.DataFrame({"id": ["A"]})
    assessor = CatalogAssessor(bad_catalog, DummyExtractor())

    with pytest.raises(ValueError, match="Missing required catalog columns"):
        assessor.group_by_title_similarity()


def test_validate_threshold_range(sample_catalog):
    assessor = CatalogAssessor(sample_catalog, DummyExtractor())

    with pytest.raises(ValueError, match="rentang 0-100"):
        assessor.group_by_title_similarity(threshold=120)
