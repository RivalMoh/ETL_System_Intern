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

    # A, B, D: isi identik (D urutan baris terbalik — harus tetap terdeteksi karena row-order independent)
    df_same_1 = pd.DataFrame({"col1": [1, 2], "col2": ["x", "y"]})
    df_same_2 = pd.DataFrame({"col1": [1, 2], "col2": ["x", "y"]})
    df_same_3 = pd.DataFrame({"col1": [2, 1], "col2": ["y", "x"]})  # urutan baris terbalik
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

    # A, B, D semuanya identik → 3 pasang: A-B, A-D, B-D
    assert len(df_dup) == 3

    # Gunakan frozenset agar assertion tidak bergantung pada urutan baris di DataFrame
    found_pairs = {
        frozenset([row["ID_Tabel_A"], row["ID_Tabel_B"]])
        for _, row in df_dup.iterrows()
    }
    assert frozenset(["A", "B"]) in found_pairs
    assert frozenset(["A", "D"]) in found_pairs
    assert frozenset(["B", "D"]) in found_pairs


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


def test_verify_near_identical_strings_detected():
    """
    Dataset dengan nilai string hampir identik (typo minor) harus terdeteksi
    saat similarity_threshold=90, tapi TIDAK terdeteksi saat threshold=100.
    Ini membuktikan fuzzy comparison bekerja dan bukan hanya exact match.
    """
    catalog = pd.DataFrame({
        "id": ["X", "Y"],
        "judul": ["Data Kemiskinan Jateng", "Kemiskinan Data Jateng"],
    })

    # X: data asli dengan nama kabupaten benar
    df_original = pd.DataFrame({
        "kabupaten": ["Semarang", "Solo", "Magelang"],
        "jumlah_miskin": ["12345", "87654", "45678"],
    })
    # Y: hampir identik — satu typo ("Semerang" bukan "Semarang")
    df_typo = pd.DataFrame({
        "kabupaten": ["Semerang", "Solo", "Magelang"],   # ← 1 karakter beda
        "jumlah_miskin": ["12345", "87654", "45678"],
    })

    extractor = DummyExtractor(mapping={
        "X": df_original.assign(dataset_id="X"),
        "Y": df_typo.assign(dataset_id="Y"),
    })

    assessor = CatalogAssessor(catalog, extractor)
    assessor.suspect_groups = [{"base_title": "data kemiskinan", "dataset_ids": ["X", "Y"]}]

    # threshold 90 → harus terdeteksi meski ada 1 typo
    df_dup_fuzzy = assessor.verify_with_data_sample(sample_size=5, similarity_threshold=90)
    assert len(df_dup_fuzzy) == 1
    assert "Skor_Kemiripan" in df_dup_fuzzy.columns
    assert df_dup_fuzzy.loc[0, "Skor_Kemiripan"] < 100.0   # bukan identik persis
    assert df_dup_fuzzy.loc[0, "Skor_Kemiripan"] >= 90.0   # tapi masih di atas threshold
    assert "hampir identik" in df_dup_fuzzy.loc[0, "Alasan_Duplikat"]

    # threshold 100 (exact only) → TIDAK terdeteksi karena ada typo
    df_dup_strict = assessor.verify_with_data_sample(sample_size=5, similarity_threshold=100)
    assert df_dup_strict.empty


def test_verify_completely_different_data_not_flagged():
    """
    Dataset dengan isi data yang benar-benar berbeda tidak boleh di-flag
    meski judul mirip, bahkan dengan threshold rendah (85).
    """
    catalog = pd.DataFrame({
        "id": ["P", "Q"],
        "judul": ["Data Padi Jateng", "Padi Data Jateng"],
    })

    df_a = pd.DataFrame({
        "kabupaten": ["Semarang", "Solo"],
        "produksi_ton": ["5000", "3000"],
    })
    df_b = pd.DataFrame({
        "kabupaten": ["Banyumas", "Purworejo"],
        "produksi_ton": ["1200", "9800"],
    })

    extractor = DummyExtractor(mapping={
        "P": df_a.assign(dataset_id="P"),
        "Q": df_b.assign(dataset_id="Q"),
    })

    assessor = CatalogAssessor(catalog, extractor)
    assessor.suspect_groups = [{"base_title": "data padi", "dataset_ids": ["P", "Q"]}]

    df_dup = assessor.verify_with_data_sample(sample_size=5, similarity_threshold=85)
    assert df_dup.empty
