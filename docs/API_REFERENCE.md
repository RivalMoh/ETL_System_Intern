# API Reference

Referensi lengkap semua kelas dan method publik di pipeline ETL.

---

## `src/config.py`

### `AppSettings`

Membaca konfigurasi dari file `.env`.

```python
settings = AppSettings()
```

| Attribute | Type | Default | Sumber `.env` |
|---|---|---|---|
| `base_url` | `str` | `None` | `BASE_URL` |
| `api_key` | `str` | `None` | `API_KEY` |
| `max_pages` | `int` | `1` | `MAX_PAGES` |
| `max_datasets` | `int` | `5` | `MAX_DATASETS_TO_ASSESS` |
| `dup_threshold` | `int` | `85` | `DUPLICATE_TITLE_THRESHOLD` |
| `dup_sample_size` | `int` | `5` | `DUPLICATE_SAMPLE_SIZE` |
| `require_columns` | `List[str]` | `["tahun", "jumlah"]` | `REQUIRED_COLUMNS` |
| `allowed_load_statuses` | `List[str]` | `["ready"]` | `LOAD_ALLOWED_STATUSES` |
| `new_base_url` | `str` | `""` | `NEW_BASE_URL` |
| `new_api_key` | `str` | `None` | `NEW_API_KEY` |
| `year_min` | `int` | `2000` | `YEAR_MIN` |
| `year_max` | `int` | `2025` | `YEAR_MAX` |

---

## `src/extract.py`

### `APIExtractor`

HTTP client untuk mengambil data dari API sistem lama.

```python
extractor = APIExtractor(base_url, api_key, max_pages=10)
```

| Method | Return | Deskripsi |
|---|---|---|
| `get_dataset_catalog()` | `pd.DataFrame` | Tarik katalog lengkap (paginated) |
| `get_dataset_details(dataset_id)` | `pd.DataFrame` | Tarik baris data per dataset |
| `close()` | `None` | Tutup HTTP session |

---

## `src/catalog_assessor.py`

### `CatalogAssessor`

Deteksi duplikat dataset berdasarkan kemiripan judul dan data.

```python
assessor = CatalogAssessor(df_catalog, extractor)
df_dup = assessor.group_by_title_similarity(threshold=85)
                 .verify_with_data_sample(sample_size=5)
```

| Method | Return | Deskripsi |
|---|---|---|
| `group_by_title_similarity(threshold)` | `self` | Kelompokkan judul mirip via `fuzz.token_sort_ratio` |
| `verify_with_data_sample(sample_size)` | `pd.DataFrame` | Verifikasi dengan fingerprint data (bidirectional fuzzy) |
| `get_skipped_rows()` | `pd.DataFrame` | Dataset yang gagal diambil saat verifikasi |

---

## `src/data_preprocessor.py`

### `DataPreprocessor`

Membersihkan DataFrame sebelum assessment. **Fluent API**.

```python
df_clean = (DataPreprocessor(df)
            .normalize_columns()
            .strip_whitespace()
            .fix_kode_wilayah()
            .get_result())
```

| Method | Return | Deskripsi |
|---|---|---|
| `normalize_columns()` | `self` | Rename kolom via `column_mapping.json` + hardcoded fallback |
| `strip_whitespace()` | `self` | Strip + collapse multi-space pada kolom string |
| `fix_kode_wilayah(column="kode_wilayah")` | `self` | Format kode BPS: `3320` → `33.20` |
| `get_result()` | `pd.DataFrame` | Kembalikan DataFrame yang sudah dibersihkan |
| `get_changes_log()` | `List[Dict]` | Log semua perubahan yang dilakukan |

---

## `src/data_assessor.py`

### `DataAssessor`

Evaluasi kualitas data per dataset. **Fluent API** (mengembalikan DataFrame).

```python
assessor = DataAssessor(df)
df_assessed = (assessor
               .standardize_year_column()
               .flag_missing_values(["tahun", "kode_wilayah"])
               .warn_suspicious_year(min_year=2000, max_year=2025)
               .mark_ready())
```

| Method | Return | Deskripsi |
|---|---|---|
| `standardize_year_column()` | `pd.DataFrame` | Normalisasi kolom tahun (int conversion) |
| `flag_missing_values(required_cols)` | `pd.DataFrame` | Flag baris dengan missing values → `migration_status = flagged` |
| `warn_suspicious_year(min_year, max_year)` | `pd.DataFrame` | Warning jika tahun di luar range (non-blocking) |
| `mark_ready()` | `pd.DataFrame` | Tandai baris yang belum di-flag sebagai `ready` |

**Kolom yang ditambahkan:**
- `migration_status`: `"ready"` / `"flagged"`
- `flag_reason`: alasan flag/warning (string, bisa multi)

---

## `src/load.py`

### `LoadGate`

Filter baris berdasarkan `migration_status`.

```python
gate = LoadGate(allowed_statuses=["ready"])
df_loadable = gate.select_rows(df_assessed)
summary = gate.build_summary(df_assessed)
```

| Method | Return | Deskripsi |
|---|---|---|
| `select_rows(df)` | `pd.DataFrame` | Baris dengan status yang diperbolehkan |
| `build_summary(df)` | `Dict` | `{loadable_rows, blocked_rows}` |

---

## `src/reporting.py`

### `ReportGenerator`

Generate output file (Excel + CSV).

```python
reporter = ReportGenerator()
reporter.generate_hybrid_report(
    df_catalog, df_duplicates, df_skipped,
    df_micro_summary, df_load_summary,
    load_ready_rows, manager_review_rows
)
```

| Method | Parameter | Output Files |
|---|---|---|
| `generate_hybrid_report(...)` | 7 positional args | `Audit_Migrasi_*.xlsx`, `Load_Ready_*.csv`, `Manager_Review_*.csv` |

---

## `src/loader/client.py`

### `TargetAPIClient`

HTTP client untuk API sistem baru (target).

```python
client = TargetAPIClient(base_url, api_key)
```

| Method | Return | Deskripsi |
|---|---|---|
| `get_catalog()` | `List[Dict]` | `GET /` → daftar tabel di target |
| `post_data(target_id, payload)` | `bool` | `POST /{target_id}` → kirim data, `True` jika sukses |
| `close()` | `None` | Tutup HTTP session |

**Payload format:**
```json
{
  "tahun_data": 2022,
  "data": [
    {"kabupaten_kota": "Semarang", "jumlah": 100},
    {"kabupaten_kota": "Solo", "jumlah": 200}
  ]
}
```

---

## `src/loader/mapper.py`

### `AutoMapper`

Mencocokkan judul dataset lama dengan ID di sistem baru.

```python
mapper = AutoMapper(threshold=85)
df_mapping = mapper.generate_mapping(df_ready, new_catalog)
```

| Method | Return | Deskripsi |
|---|---|---|
| `generate_mapping(df_ready, new_catalog)` | `pd.DataFrame` | Mapping `old_id` ↔ `new_id` dengan `match_score` |

**Output DataFrame columns:** `old_id, old_title, new_id, new_title, match_score`

**Side effects:**
- `data/reports/auto_mapping_result.csv` — hasil mapping
- `data/reports/unmapped_datasets.csv` — dataset tanpa kecocokan

---

## `src/loader/column_normalizer.py`

### `ColumnNormalizer`

Rename nama kolom dari format legacy ke standar target.

```python
normalizer = ColumnNormalizer(
    mapping_file="data/column_mapping.json",
    fuzzy_threshold=80,
    target_columns=["kabupaten_kota", "jumlah"]  # optional
)
result = normalizer.normalize_record({"kab_kota": "Solo"}, dataset_id="ds1")
# → {"kabupaten_kota": "Solo"}
```

| Method | Return | Deskripsi |
|---|---|---|
| `normalize_record(record, dataset_id)` | `Dict` | Dict baru dengan key yang sudah dinormalisasi |
| `get_rename_report()` | `List[Dict]` | Log semua rename yang dilakukan |
| `save_rename_report(output_path)` | `None` | Simpan log ke CSV |

**Matching priority:** Explicit alias → Already standard → Known target → Fuzzy match → Lowercase fallback

---

## `src/loader/transform.py`

### `MigrationTransformer`

Mengubah `df_ready` menjadi list of payload siap POST.

```python
transformer = MigrationTransformer(df_mapping, column_normalizer=normalizer)
payloads = transformer.build_payloads(df_ready)
```

| Method | Return | Deskripsi |
|---|---|---|
| `build_payloads(df_ready)` | `List[Dict]` | List payload `{target_id, body}` dikelompokkan per tahun |

---

## `src/loader/progress_tracker.py`

### `MigrationProgressTracker`

Persist status migrasi per tabel antar batch.

```python
tracker = MigrationProgressTracker(progress_file="data/reports/migration_progress.csv")
```

| Method | Return | Deskripsi |
|---|---|---|
| `get_done_ids()` | `Set[str]` | Set `new_id` yang sudah `done` |
| `get_next_batch_number()` | `int` | Auto-increment dari max batch |
| `record(new_id, new_title, old_id, status, rows_sent, batch_number)` | `None` | Upsert satu baris progress |
| `get_summary()` | `Dict` | `{total_recorded, done, partial, failed}` |
| `log_catalog_status(new_catalog)` | `None` | Log status setiap tabel ke logger |

**Constants:**
- `STATUS_DONE = "done"`
- `STATUS_PARTIAL = "partial"`
- `STATUS_FAILED = "failed"`

---

## `src/pipeline.py`

### `MigrationPipeline`

Orkestrator mode audit.

```python
pipeline = MigrationPipeline(settings)
pipeline.run()
```

---

## `src/loader/pipeline.py`

### `MigrationLoadPipeline`

Orkestrator mode migrate (stateful).

```python
pipeline = MigrationLoadPipeline(settings)
pipeline.run(ready_csv_path="data/reports/Load_Ready_*.csv")
```
