# Developer Guide

Panduan untuk developer yang baru bergabung atau ingin melanjutkan pengembangan pipeline ETL ini.

---

## Setup Lingkungan Pengembangan

### Prasyarat

- Python 3.10+
- Git
- Text editor / IDE (VS Code direkomendasikan)

### Instalasi

```bash
# 1. Clone repositori
git clone <repo_url>
cd ETL_Pipeline_Satu-Data-Jateng

# 2. Buat virtual environment
python -m venv .venv

# Aktifkan:
# Windows:
.venv\Scripts\activate
# Mac/Linux:
source .venv/bin/activate

# 3. Install dependensi
pip install -r requirements.txt

# 4. Setup konfigurasi
cp .env.example .env
# Edit .env dengan kredensial yang benar
```

### Verifikasi Instalasi

```bash
# Jalankan semua test
pytest tests/ -v

# Harus muncul: "114 passed"
```

---

## Struktur Kode

```
src/
├── config.py              # Settings dari .env
├── extract.py             # API client (GET paginated)
├── catalog_assessor.py    # Macro: fuzzy title + data fingerprint
├── data_preprocessor.py   # Clean: rename, whitespace, kode_wilayah
├── data_assessor.py       # Micro: flag, warn, mark ready
├── load.py                # Filter by migration_status
├── reporting.py           # Generate Excel + CSV
├── pipeline.py            # Orkestrator audit
└── loader/                # Sub-package untuk migrasi
    ├── client.py              # HTTP client API target
    ├── mapper.py              # Fuzzy matching title → ID
    ├── column_normalizer.py   # Rename kolom sebelum POST
    ├── transform.py           # Build payload {tahun_data, data}
    ├── progress_tracker.py    # Persist status antar batch
    └── pipeline.py            # Orkestrator migrate
```

---

## Konvensi Kode

### Penamaan

- **File**: `snake_case.py`
- **Class**: `PascalCase` (misal `DataPreprocessor`, `MigrationLoadPipeline`)
- **Method/Function**: `snake_case` (misal `build_payloads`, `get_done_ids`)
- **Konstanta**: `UPPER_SNAKE_CASE` (misal `STATUS_DONE`, `_TAHUN_PATTERNS`)
- **Private method**: prefix `_` (misal `_load_mapping`, `_fuzzy_find`)

### Pola Desain yang Digunakan

1. **Fluent API (Builder Pattern)** — `DataPreprocessor` dan `DataAssessor`:
   ```python
   df_clean = (DataPreprocessor(df)
               .normalize_columns()
               .strip_whitespace()
               .fix_kode_wilayah()
               .get_result())
   ```

2. **Strategy Pattern** — Matching di `ColumnNormalizer`:
   - Explicit alias → Known target → Fuzzy match → Fallback

3. **Upsert Pattern** — `ProgressTracker.record()`:
   - Drop existing row → Append new → Save

### Logging

Semua modul menggunakan Python `logging` standar:
```python
import logging
logger = logging.getLogger(__name__)

# Levels yang digunakan:
logger.info(...)      # Progress normal
logger.warning(...)   # Ada masalah tapi bisa dilanjutkan
logger.error(...)     # Gagal, operasi dibatalkan
logger.debug(...)     # Detail untuk debugging
```

Output log ditulis ke:
- `logs/etl-pipeline.log` (file)
- `stdout` (terminal)

---

## Menambah Fitur Baru

### Menambah Alias Kolom Baru

Edit `data/column_mapping.json`:
```json
{
  "column_aliases": {
    "nama_kolom_lama": "nama_standar",
    ...
  }
}
```

**Tidak perlu mengubah kode Python.** File ini digunakan oleh `DataPreprocessor` (audit) dan `ColumnNormalizer` (migrate).

### Menambah Kolom Wajib Baru

Edit `.env`:
```env
REQUIRED_COLUMNS=tahun,kode_wilayah,nama_kolom_baru
```

### Menambah Preprocessing Step Baru

1. Tambah method baru di `src/data_preprocessor.py`:
   ```python
   def fix_something_new(self) -> "DataPreprocessor":
       # ... logic ...
       return self  # fluent API
   ```

2. Chain di `src/pipeline.py`:
   ```python
   df_clean = (preprocessor
               .normalize_columns()
               .strip_whitespace()
               .fix_kode_wilayah()
               .fix_something_new()  # ← tambah di sini
               .get_result())
   ```

3. Tambah test di `tests/test_data_preprocessor.py`

### Menambah Validasi Baru (Assessment)

1. Tambah method di `src/data_assessor.py`:
   ```python
   def warn_something(self) -> pd.DataFrame:
       # ... logic ...
       return self.df
   ```

2. Chain di `src/pipeline.py` setelah `mark_ready()`

3. Tambah test di `tests/test_data_assess.py`

---

## Testing

### Menjalankan Tests

```bash
# Semua test
pytest tests/ -v

# Test tertentu
pytest tests/test_column_normalizer.py -v

# Dengan coverage
pytest tests/ --cov=src --cov-report=term-missing

# Hanya test yang gagal terakhir kali
pytest tests/ --lf
```

### Struktur Test

| File | Scope | Apa yang Ditest |
|---|---|---|
| `test_catalog_assessor.py` | Unit | Fuzzy grouping, fingerprint |
| `test_data_preprocessor.py` | Unit | Rename, whitespace, kode_wilayah |
| `test_data_assess.py` | Unit | Flag, warn, standardize |
| `test_column_normalizer.py` | Unit | Alias, fuzzy, cache, report |
| `test_extract.py` | Unit | Paginated extraction |
| `test_load.py` | Unit | Status filtering |
| `test_loader_client.py` | Unit | HTTP client (mocked) |
| `test_mapper.py` | Unit | Fuzzy title → ID mapping |
| `test_progress_tracker.py` | Unit | State persistence, upsert |
| `test_transform.py` | Unit | Payload building |
| `test_system_audit.py` | **System** | End-to-end audit pipeline |
| `test_system_migrate.py` | **System** | End-to-end migrate pipeline |

### Konvensi Test

- Satu file test per modul source
- Test function name: `test_<what>_<expected_behavior>`
- Gunakan `tmp_path` fixture untuk file sementara
- Mock semua HTTP calls (tidak ada test yang memerlukan koneksi API)
- Shared fixtures di `tests/conftest.py`

### Menulis Test Baru

```python
def test_new_feature_does_expected_thing():
    """Deskripsi singkat apa yang ditest."""
    # Arrange
    input_data = ...

    # Act
    result = my_function(input_data)

    # Assert
    assert result == expected, "pesan error jika gagal"
```

---

## Debugging

### Masalah Umum saat Development

| Gejala | Penyebab | Solusi |
|---|---|---|
| `ModuleNotFoundError: src.*` | Belum di root directory | Jalankan dari root proyek |
| Test gagal dengan `FileNotFoundError` | `column_mapping.json` tidak ada | Gunakan `tmp_path` di test |
| `.env` tidak terbaca | `.env` bukan di root | Pastikan file ada di root proyek |
| Unicode error di Windows | `→` atau emoji di log | Gunakan ASCII (`->`) di log message |

### Tips Debugging

1. **Lihat log file** → `logs/etl-pipeline.log`
2. **Jalankan satu test** → `pytest tests/test_xxx.py::test_specific_name -v -s`
3. **Print di test** → tambah flag `-s` agar `print()` muncul
4. **Breakpoint** → tambah `breakpoint()` di kode, jalankan test tanpa `-s`

---

## Git Workflow

### Branch Convention

- `main` — kode stabil, sudah ditest
- `dev` — development aktif
- `feature/<nama>` — fitur baru
- `fix/<nama>` — bug fix

### Commit Message

```
<type>: <deskripsi singkat>

Contoh:
feat: tambah warn_suspicious_year di DataAssessor
fix: progress_tracker ValueError pada duplicate index
refactor: split pipeline.py menjadi audit + migrate
docs: tambah ARCHITECTURE.md
test: tambah regression test progress_tracker upsert
```
