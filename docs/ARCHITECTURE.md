# Arsitektur Sistem

## Ringkasan

Pipeline ETL ini terbagi menjadi **dua mode operasi independen** yang masing-masing berjalan sebagai proses terpisah:

1. **Mode Audit** — Mengambil data dari sistem lama, membersihkan, mengevaluasi kualitas, dan menghasilkan laporan
2. **Mode Migrate** — Membaca output audit, memetakan ke sistem baru, dan mengirim data via API secara stateful

Kedua mode dijembatani oleh file CSV (`Load_Ready_*.csv`) yang berfungsi sebagai *data contract* antara audit dan migrasi.

---

## Diagram Arsitektur Tingkat Tinggi

```
┌──────────────────────────────────────────────────────────────────────────┐
│                           main.py (CLI)                                  │
│                    --mode audit | --mode migrate                         │
└─────────────┬─────────────────────────────────┬──────────────────────────┘
              │                                 │
              ▼                                 ▼
┌─────────────────────────┐      ┌──────────────────────────────┐
│   MigrationPipeline     │      │   MigrationLoadPipeline      │
│   (src/pipeline.py)     │      │   (src/loader/pipeline.py)   │
│                         │      │                              │
│  ┌─────────────────┐    │      │  ┌────────────────────────┐  │
│  │  APIExtractor   │    │      │  │  TargetAPIClient       │  │
│  │  (extract.py)   │    │      │  │  (loader/client.py)    │  │
│  └────────┬────────┘    │      │  └────────────────────────┘  │
│           │             │      │                              │
│  ┌────────▼────────┐    │      │  ┌────────────────────────┐  │
│  │ CatalogAssessor │    │      │  │  AutoMapper            │  │
│  │ (catalog_*.py)  │    │      │  │  (loader/mapper.py)    │  │
│  └────────┬────────┘    │      │  └────────────────────────┘  │
│           │             │      │                              │
│  ┌────────▼────────┐    │      │  ┌────────────────────────┐  │
│  │DataPreprocessor │    │      │  │  ColumnNormalizer       │  │
│  │(data_preproc.py)│    │      │  │  (loader/col_norm.py)  │  │
│  └────────┬────────┘    │      │  └────────────────────────┘  │
│           │             │      │                              │
│  ┌────────▼────────┐    │      │  ┌────────────────────────┐  │
│  │  DataAssessor   │    │      │  │  MigrationTransformer  │  │
│  │ (data_assess.py)│    │      │  │  (loader/transform.py) │  │
│  └────────┬────────┘    │      │  └────────────────────────┘  │
│           │             │      │                              │
│  ┌────────▼────────┐    │      │  ┌────────────────────────┐  │
│  │   LoadGate      │    │      │  │  ProgressTracker       │  │
│  │   (load.py)     │    │      │  │  (loader/progress_*.py)│  │
│  └────────┬────────┘    │      │  └────────────────────────┘  │
│           │             │      │                              │
│  ┌────────▼────────┐    │      └──────────────────────────────┘
│  │ ReportGenerator │    │
│  │ (reporting.py)  │    │
│  └─────────────────┘    │
└─────────────────────────┘
```

---

## Komponen per Mode

### Mode Audit

| Komponen | File | Tanggung Jawab |
|---|---|---|
| `AppSettings` | `src/config.py` | Membaca `.env`, validasi range, parsing list |
| `APIExtractor` | `src/extract.py` | HTTP client ke API lama, paginated catalog, detail per dataset |
| `CatalogAssessor` | `src/catalog_assessor.py` | Fuzzy title grouping + data fingerprint verification |
| `DataPreprocessor` | `src/data_preprocessor.py` | Rename kolom, strip whitespace, fix kode_wilayah |
| `DataAssessor` | `src/data_assessor.py` | Flag missing values, warn year range, mark ready |
| `LoadGate` | `src/load.py` | Filter baris berdasarkan `migration_status` |
| `ReportGenerator` | `src/reporting.py` | Generate Excel (5 sheet) + CSV output |
| `MigrationPipeline` | `src/pipeline.py` | **Orkestrator** — menjalankan semua komponen di atas |

### Mode Migrate

| Komponen | File | Tanggung Jawab |
|---|---|---|
| `TargetAPIClient` | `src/loader/client.py` | HTTP client ke API target (GET catalog, POST data) |
| `AutoMapper` | `src/loader/mapper.py` | Fuzzy matching judul dataset lama ↔ ID target |
| `ColumnNormalizer` | `src/loader/column_normalizer.py` | Rename kolom payload sebelum POST |
| `MigrationTransformer` | `src/loader/transform.py` | Parse JSON → group by tahun → build payload |
| `MigrationProgressTracker` | `src/loader/progress_tracker.py` | Persist status `done/partial/failed` antar batch |
| `MigrationLoadPipeline` | `src/loader/pipeline.py` | **Orkestrator** — stateful batch migration |

---

## Alur Data

### Mode Audit: Data Flow

```
API Lama (paginated GET)
    │
    ▼
df_catalog (id, judul, ...)
    │
    ├── CatalogAssessor.group_by_title_similarity()
    │       → df_duplicates (pasangan suspect)
    │       → duplicate_ids (set of suspect IDs)
    │
    ▼ per dataset:
df_detail (raw data dari API)
    │
    ├── DataPreprocessor
    │       .normalize_columns()    ← column_mapping.json
    │       .strip_whitespace()     ← collapse multi-space
    │       .fix_kode_wilayah()     ← format BPS
    │       → df_clean
    │
    ├── DataAssessor
    │       .standardize_year_column()
    │       .flag_missing_values()  ← REQUIRED_COLUMNS
    │       .warn_suspicious_year() ← YEAR_MIN/YEAR_MAX
    │       .mark_ready()
    │       → df_assessed (+ migration_status, flag_reason)
    │
    ├── Routing Decision
    │       is_suspect OR flagged_rows > 0
    │           → manager_review_required → Manager_Review.csv
    │       else
    │           → ready_for_load → Load_Ready.csv
    │
    └── ReportGenerator
            → Audit_Migrasi_*.xlsx (5 sheets)
            → Load_Ready_*.csv
            → Manager_Review_*.csv
```

### Mode Migrate: Data Flow

```
Load_Ready_*.csv (output audit)
    │
    ▼
migration_progress.csv
    │ get_done_ids() → exclude done
    │
    ▼
API Target (GET /data → catalog)
    │
    ├── AutoMapper.generate_mapping()
    │       → df_mapping (old_id ↔ new_id)
    │
    ├── ColumnNormalizer
    │       → rename keys per record
    │
    ├── MigrationTransformer.build_payloads()
    │       → list of {target_id, body: {tahun_data, data: [...]}}
    │
    ├── POST per payload ke API Target
    │       → success/fail per tahun per dataset
    │
    ├── ProgressTracker.record()
    │       → done / partial / failed
    │
    └── Reports
            → column_mapping_report.csv
            → failed_payloads_batch*.csv
            → migration_progress.csv (updated)
```

---

## Dependency Graph (Imports)

```
main.py
  ├── src.config.AppSettings
  ├── src.pipeline.MigrationPipeline
  │     ├── src.extract.APIExtractor
  │     ├── src.catalog_assessor.CatalogAssessor
  │     ├── src.data_preprocessor.DataPreprocessor
  │     ├── src.data_assessor.DataAssessor
  │     ├── src.load.LoadGate
  │     └── src.reporting.ReportGenerator
  │
  └── src.loader.pipeline.MigrationLoadPipeline
        ├── src.loader.client.TargetAPIClient
        ├── src.loader.mapper.AutoMapper
        ├── src.loader.column_normalizer.ColumnNormalizer
        ├── src.loader.transform.MigrationTransformer
        └── src.loader.progress_tracker.MigrationProgressTracker
```

---

## State Management

### File Persisten

| File | Tujuan | Lifecycle |
|---|---|---|
| `.env` | Konfigurasi kredensial + parameter | Statis, set manual |
| `column_mapping.json` | Alias kolom | Statis, edit manual saat ada kolom baru |
| `migration_progress.csv` | Status migrasi per tabel | Bertambah setiap batch, **jangan hapus** jika ingin resume |
| `Load_Ready_*.csv` | Data contract audit → migrate | Dibuat oleh audit, dibaca oleh migrate |
| `logs/etl-pipeline.log` | Log eksekusi | Append-only |

### Stateful Batch Pattern

```
Batch 1:
  progress.csv = []
  → POST semua dataset
  → record: {A: done, B: partial, C: failed}

Batch 2:
  progress.csv = [{A: done}, {B: partial}, {C: failed}]
  → skip A (done)
  → retry B dan C
  → record: {A: done, B: done, C: partial}

Batch 3:
  → skip A dan B
  → retry C
  → ...
```

---

## Keputusan Arsitektur

### 1. Pemisahan Audit dan Migrate

**Alasan:** Data harus divalidasi oleh manusia (manager) sebelum dikirim ke sistem produksi. Audit menghasilkan file CSV yang bisa di-review sebelum migrate dijalankan.

### 2. CSV sebagai Data Contract (bukan Database)

**Alasan:** Proyek ini adalah pipeline migrasi satu kali (one-time migration), bukan sistem operasional jangka panjang. CSV cukup untuk skala ini dan tidak memerlukan setup database.

### 3. Fuzzy Matching (thefuzz) untuk Title Deduplication

**Alasan:** Dataset di sistem lama sering memiliki judul yang mirip tapi urutannya berbeda ("Data Padi Jawa Tengah" vs "Jawa Tengah Data Padi"). `token_sort_ratio` menangani ini dengan baik.

### 4. Drop-then-Append untuk Upsert (ProgressTracker)

**Alasan:** `pd.DataFrame.loc[mask, k] = v` crash dengan `ValueError: cannot reindex` ketika index memiliki duplikat label (terjadi setelah beberapa `pd.concat`). Drop-then-append lebih robust.

### 5. Column Normalizer berbasis JSON Config

**Alasan:** API target tidak menyediakan endpoint schema/metadata kolom. `column_mapping.json` memungkinkan developer menambah alias baru tanpa mengubah kode.
