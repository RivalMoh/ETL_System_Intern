"""
Unit tests untuk AutoMapper (src/loader/mapper.py).

Menggunakan tmp_path agar output CSV tidak mengotori direktori proyek saat testing.
"""
import os
import pytest
import pandas as pd
from unittest.mock import patch

from src.loader.mapper import AutoMapper


# ─── Helpers ─────────────────────────────────────────────────────────────────


def make_df_ready(*titles_with_ids):
    """Buat df_ready minimal dengan kolom Dataset_Id dan Judul_Tabel."""
    rows = [{"Dataset_Id": str(i + 1), "Judul_Tabel": title}
            for i, title in enumerate(titles_with_ids)]
    return pd.DataFrame(rows)


def make_catalog(*titles_with_ids):
    """Buat new_catalog sebagai list of dict dengan key id & judul."""
    return [{"id": str(i + 100), "judul": title}
            for i, title in enumerate(titles_with_ids)]


# ─── Input Validation ────────────────────────────────────────────────────────


def test_missing_required_columns_raises_valueerror(tmp_path):
    """df_ready tanpa kolom Dataset_Id / Judul_Tabel harus raise ValueError."""
    mapper = AutoMapper(threshold=85)
    df_bad = pd.DataFrame({"wrong_col": ["x"]})

    with patch("src.loader.mapper.os.makedirs"):
        with pytest.raises(ValueError, match="missing required columns"):
            mapper.generate_mapping(df_bad, new_catalog=[])


def test_empty_new_catalog_returns_empty_dataframe(tmp_path):
    """Jika new_catalog kosong, tidak ada yang bisa di-mapping → DataFrame kosong."""
    mapper = AutoMapper(threshold=85)
    df_ready = make_df_ready("Data Padi Jateng")

    with patch("src.loader.mapper.os.makedirs"), \
         patch.object(pd.DataFrame, "to_csv"):
        result = mapper.generate_mapping(df_ready, new_catalog=[])

    assert result.empty


# ─── Matching Logic ───────────────────────────────────────────────────────────


def test_exact_title_match_is_mapped(tmp_path):
    """Judul yang identik harus lolos threshold dan masuk ke mapping."""
    mapper = AutoMapper(threshold=85)
    df_ready = make_df_ready("Data Padi Jawa Tengah")
    new_catalog = make_catalog("Data Padi Jawa Tengah")

    with patch("src.loader.mapper.os.makedirs"), \
         patch.object(pd.DataFrame, "to_csv"):
        result = mapper.generate_mapping(df_ready, new_catalog)

    assert len(result) == 1
    assert result.iloc[0]["old_id"] == "1"
    assert result.iloc[0]["new_id"] == "100"
    assert result.iloc[0]["match_score"] == 100


def test_reordered_tokens_still_match():
    """token_sort_ratio harus match meski urutan kata berbeda."""
    mapper = AutoMapper(threshold=85)
    df_ready = make_df_ready("Jawa Tengah Data Padi")  # urutan berbeda
    new_catalog = make_catalog("Data Padi Jawa Tengah")

    with patch("src.loader.mapper.os.makedirs"), \
         patch.object(pd.DataFrame, "to_csv"):
        result = mapper.generate_mapping(df_ready, new_catalog)

    assert len(result) == 1
    assert result.iloc[0]["match_score"] == 100


def test_below_threshold_goes_to_unmapped():
    """Judul yang tidak cukup mirip tidak boleh masuk ke mapping."""
    mapper = AutoMapper(threshold=85)
    df_ready = make_df_ready("Data Kemiskinan")
    new_catalog = make_catalog("Produksi Ikan Laut")  # sangat berbeda

    with patch("src.loader.mapper.os.makedirs"), \
         patch.object(pd.DataFrame, "to_csv"):
        result = mapper.generate_mapping(df_ready, new_catalog)

    assert result.empty


def test_best_match_wins_when_multiple_candidates():
    """Jika ada banyak kandidat, hanya yang skor tertinggi yang dipilih."""
    mapper = AutoMapper(threshold=85)
    df_ready = make_df_ready("Data Padi Jawa Tengah")
    new_catalog = [
        {"id": "200", "judul": "Data Jagung Jawa Barat"},   # tidak mirip
        {"id": "201", "judul": "Data Padi Jawa Tengah"},    # identik
        {"id": "202", "judul": "Data Padi Jawa"},            # mirip tapi tidak sebaik 201
    ]

    with patch("src.loader.mapper.os.makedirs"), \
         patch.object(pd.DataFrame, "to_csv"):
        result = mapper.generate_mapping(df_ready, new_catalog)

    assert len(result) == 1
    assert result.iloc[0]["new_id"] == "201"


def test_multiple_datasets_mapped_independently():
    """Beberapa dataset lama di-mapping secara independen."""
    mapper = AutoMapper(threshold=85)
    df_ready = make_df_ready("Data Padi", "Data Jagung")
    new_catalog = make_catalog("Data Padi", "Data Jagung", "Data Kedelai")

    with patch("src.loader.mapper.os.makedirs"), \
         patch.object(pd.DataFrame, "to_csv"):
        result = mapper.generate_mapping(df_ready, new_catalog)

    assert len(result) == 2
    mapped_old_ids = set(result["old_id"].tolist())
    assert mapped_old_ids == {"1", "2"}


def test_result_dataframe_has_required_columns():
    """DataFrame hasil mapping harus punya kolom old_id, new_id, match_score."""
    mapper = AutoMapper(threshold=85)
    df_ready = make_df_ready("Data Padi")
    new_catalog = make_catalog("Data Padi")

    with patch("src.loader.mapper.os.makedirs"), \
         patch.object(pd.DataFrame, "to_csv"):
        result = mapper.generate_mapping(df_ready, new_catalog)

    for col in ["old_id", "new_id", "old_title", "new_title", "match_score"]:
        assert col in result.columns, f"Kolom '{col}' tidak ada di hasil mapping"


# ─── Collision Detection ─────────────────────────────────────────────────────


def test_collision_warning_logged_when_same_new_id_mapped_twice(caplog):
    """
    Jika dua old_id berbeda ke-mapping ke new_id yang sama (collision),
    harus ada WARNING di log.
    """
    mapper = AutoMapper(threshold=85)
    # Dua judul lama yang mirip dengan SATU judul baru → potensi collision
    df_ready = pd.DataFrame({
        "Dataset_Id": ["1", "2"],
        "Judul_Tabel": ["Data Padi Jateng 2020", "Data Padi Jateng 2021"],
    })
    new_catalog = [{"id": "100", "judul": "Data Padi Jateng"}]  # hanya 1 target

    import logging
    with patch("src.loader.mapper.os.makedirs"), \
         patch.object(pd.DataFrame, "to_csv"), \
         caplog.at_level(logging.WARNING, logger="src.loader.mapper"):
        mapper.generate_mapping(df_ready, new_catalog)

    assert any("collision" in msg.lower() or "sama" in msg.lower()
                for msg in caplog.messages), \
        "Tidak ada warning collision padahal 2 old_id mapping ke 1 new_id"


# ─── Unmapped Report ─────────────────────────────────────────────────────────


def test_unmapped_datasets_are_saved_to_csv(tmp_path):
    """Dataset yang tidak ter-mapping harus disimpan ke unmapped_datasets.csv."""
    mapper = AutoMapper(threshold=85)
    df_ready = make_df_ready("Data Kemiskinan")  # tidak ada padanannya
    new_catalog = make_catalog("Produksi Padi")

    # Arahkan output ke tmp_path agar tidak menulis ke disk proyek
    with patch("src.loader.mapper.os.makedirs"), \
         patch("src.loader.mapper.pd.DataFrame.to_csv") as mock_csv:
        mapper.generate_mapping(df_ready, new_catalog)

    # to_csv harus dipanggil untuk unmapped file
    mock_csv.assert_called()
    saved_path = mock_csv.call_args_list[0][0][0]
    assert "unmapped" in saved_path
