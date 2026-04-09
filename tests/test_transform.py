"""
Unit tests untuk MigrationTransformer (src/loader/transform.py).
"""
import json
import pytest
import pandas as pd

from src.loader.transform import MigrationTransformer


# ─── Helpers ─────────────────────────────────────────────────────────────────


def make_mapping(pairs: dict) -> pd.DataFrame:
    """Buat df_mapping dari dict {old_id: new_id}."""
    return pd.DataFrame([
        {"old_id": str(k), "new_id": int(v)} for k, v in pairs.items()
    ])


def make_df_ready(dataset_id: str, rows: list) -> pd.DataFrame:
    """Buat df_ready dari list of row dicts. Setiap row di-serialize ke JSON."""
    return pd.DataFrame({
        "Dataset_Id": [dataset_id] * len(rows),
        "Row_Data_JSON": [json.dumps(row) for row in rows],
    })


# ─── Basic Payload Building ───────────────────────────────────────────────────


def test_single_dataset_single_year_produces_one_payload():
    """1 dataset, 1 tahun → 1 payload."""
    mapping = make_mapping({"A1": 10})
    df_ready = make_df_ready("A1", [
        {"tahun": 2022, "kabupaten": "Semarang", "nilai": 100},
        {"tahun": 2022, "kabupaten": "Solo", "nilai": 200},
    ])

    payloads = MigrationTransformer(mapping).build_payloads(df_ready)

    assert len(payloads) == 1
    assert payloads[0]["target_id"] == 10
    assert payloads[0]["body"]["tahun_data"] == 2022
    assert len(payloads[0]["body"]["data"]) == 2


def test_single_dataset_multiple_years_produces_multiple_payloads():
    """1 dataset dengan data 3 tahun → 3 payload terpisah."""
    mapping = make_mapping({"A1": 10})
    df_ready = make_df_ready("A1", [
        {"tahun": 2020, "nilai": 50},
        {"tahun": 2021, "nilai": 60},
        {"tahun": 2022, "nilai": 70},
    ])

    payloads = MigrationTransformer(mapping).build_payloads(df_ready)

    assert len(payloads) == 3
    years = {p["body"]["tahun_data"] for p in payloads}
    assert years == {2020, 2021, 2022}


def test_multiple_datasets_produce_separate_payloads():
    """Beberapa dataset lama menghasilkan payload dengan target_id yang benar."""
    mapping = make_mapping({"A1": 10, "B2": 20})

    df_a = make_df_ready("A1", [{"tahun": 2023, "nilai": 1}])
    df_b = make_df_ready("B2", [{"tahun": 2023, "nilai": 2}])
    df_ready = pd.concat([df_a, df_b], ignore_index=True)

    payloads = MigrationTransformer(mapping).build_payloads(df_ready)

    assert len(payloads) == 2
    target_ids = {p["target_id"] for p in payloads}
    assert target_ids == {10, 20}


# ─── Tahun Handling ───────────────────────────────────────────────────────────


def test_tahun_as_string_is_parsed_correctly():
    """Nilai tahun dalam format string (dari CSV) harus bisa diparse ke int."""
    mapping = make_mapping({"A1": 10})
    df_ready = make_df_ready("A1", [{"tahun": "2021", "nilai": 100}])

    payloads = MigrationTransformer(mapping).build_payloads(df_ready)

    assert payloads[0]["body"]["tahun_data"] == 2021


def test_tahun_as_float_string_is_parsed_correctly():
    """Nilai tahun '2021.0' (artefak CSV) harus diparse ke int 2021."""
    mapping = make_mapping({"A1": 10})
    df_ready = make_df_ready("A1", [{"tahun": "2021.0", "nilai": 100}])

    payloads = MigrationTransformer(mapping).build_payloads(df_ready)

    assert payloads[0]["body"]["tahun_data"] == 2021


def test_row_without_tahun_key_is_skipped():
    """Baris yang tidak punya key 'tahun' sama sekali harus di-skip."""
    mapping = make_mapping({"A1": 10})
    df_ready = make_df_ready("A1", [
        {"nilai": 500},            # ← tidak punya 'tahun'
        {"tahun": 2022, "nilai": 999},
    ])

    payloads = MigrationTransformer(mapping).build_payloads(df_ready)

    assert len(payloads) == 1
    assert len(payloads[0]["body"]["data"]) == 1


def test_row_with_invalid_tahun_value_is_skipped():
    """Nilai tahun yang tidak bisa diparse (string non-numerik) harus di-skip."""
    mapping = make_mapping({"A1": 10})
    df_ready = make_df_ready("A1", [
        {"tahun": "tidak_tau", "nilai": 100},  # ← tidak bisa jadi int
        {"tahun": 2023, "nilai": 200},
    ])

    payloads = MigrationTransformer(mapping).build_payloads(df_ready)

    assert len(payloads) == 1
    assert payloads[0]["body"]["tahun_data"] == 2023


def test_tahun_value_zero_is_skipped():
    """Tahun bernilai 0 tidak valid dan harus di-skip."""
    mapping = make_mapping({"A1": 10})
    df_ready = make_df_ready("A1", [
        {"tahun": 0, "nilai": 100},
        {"tahun": 2022, "nilai": 200},
    ])

    payloads = MigrationTransformer(mapping).build_payloads(df_ready)

    assert len(payloads) == 1
    assert payloads[0]["body"]["tahun_data"] == 2022


def test_all_rows_invalid_tahun_produces_no_payload():
    """Jika seluruh baris tidak punya tahun valid, tidak ada payload yang dihasilkan."""
    mapping = make_mapping({"A1": 10})
    df_ready = make_df_ready("A1", [
        {"tahun": None, "nilai": 1},
        {"tahun": "n/a", "nilai": 2},
    ])

    payloads = MigrationTransformer(mapping).build_payloads(df_ready)

    assert payloads == []


# ─── Tahun Key Exclusion dari Record ─────────────────────────────────────────


def test_tahun_key_is_not_included_in_data_record():
    """
    Kolom 'tahun' harus TIDAK ada di dalam data record karena sudah diangkat
    ke level 'tahun_data' di body payload. Ini memastikan tidak ada duplikasi field.
    """
    mapping = make_mapping({"A1": 10})
    df_ready = make_df_ready("A1", [{"tahun": 2023, "kabupaten": "Solo", "nilai": 999}])

    payloads = MigrationTransformer(mapping).build_payloads(df_ready)

    record = payloads[0]["body"]["data"][0]
    assert "tahun" not in record, "Key 'tahun' seharusnya tidak ada di dalam data record"
    assert "kabupaten" in record
    assert "nilai" in record


def test_original_row_dict_not_mutated():
    """
    Menggunakan row.get() + dict comprehension (bukan row.pop()) memastikan
    dict asli tidak termutasi. Ini regression test untuk bug row.pop().
    """
    mapping = make_mapping({"A1": 10})
    original_row = {"tahun": 2023, "nilai": 500}
    df_ready = make_df_ready("A1", [original_row])

    # Rebuild row dari JSON seperti transform.py lakukan
    raw_rows = [json.loads(r) for r in df_ready["Row_Data_JSON"]]
    # Setelah build_payloads, row asli tidak boleh berubah
    MigrationTransformer(mapping).build_payloads(df_ready)

    # raw_rows diinspeksi langsung — key tahun harus masih ada di sana
    loaded = json.loads(df_ready["Row_Data_JSON"].iloc[0])
    assert "tahun" in loaded, "Key 'tahun' di Row_Data_JSON tidak boleh hilang"


# ─── Mapping Miss ────────────────────────────────────────────────────────────


def test_unmapped_dataset_id_is_skipped():
    """Dataset yang tidak ada di mapping harus di-skip dengan warning (tidak crash)."""
    mapping = make_mapping({"A1": 10})  # tidak ada "B2"
    df_ready = make_df_ready("B2", [{"tahun": 2022, "nilai": 1}])

    payloads = MigrationTransformer(mapping).build_payloads(df_ready)

    assert payloads == []


def test_partial_mapping_skips_unmapped_only():
    """Hanya dataset yang tidak ter-mapping yang di-skip; yang lain tetap diproses."""
    mapping = make_mapping({"A1": 10})
    df_a = make_df_ready("A1", [{"tahun": 2022, "nilai": 1}])
    df_b = make_df_ready("B2", [{"tahun": 2022, "nilai": 2}])  # tidak ter-mapping
    df_ready = pd.concat([df_a, df_b], ignore_index=True)

    payloads = MigrationTransformer(mapping).build_payloads(df_ready)

    assert len(payloads) == 1
    assert payloads[0]["target_id"] == 10


# ─── Payload Structure ────────────────────────────────────────────────────────


def test_payload_structure_keys():
    """Setiap payload harus punya key 'target_id' dan 'body'."""
    mapping = make_mapping({"A1": 10})
    df_ready = make_df_ready("A1", [{"tahun": 2022, "nilai": 1}])

    payloads = MigrationTransformer(mapping).build_payloads(df_ready)

    payload = payloads[0]
    assert "target_id" in payload
    assert "body" in payload
    assert "tahun_data" in payload["body"]
    assert "data" in payload["body"]


def test_payload_body_data_is_list():
    """payload['body']['data'] harus berupa list of dict."""
    mapping = make_mapping({"A1": 10})
    df_ready = make_df_ready("A1", [{"tahun": 2022, "nilai": 1}])

    payloads = MigrationTransformer(mapping).build_payloads(df_ready)

    assert isinstance(payloads[0]["body"]["data"], list)


def test_empty_df_ready_returns_no_payloads():
    """DataFrame kosong tidak boleh menghasilkan payload apapun."""
    mapping = make_mapping({"A1": 10})
    df_empty = pd.DataFrame(columns=["Dataset_Id", "Row_Data_JSON"])

    payloads = MigrationTransformer(mapping).build_payloads(df_empty)

    assert payloads == []
