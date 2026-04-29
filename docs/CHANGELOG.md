# Changelog

Riwayat semua perubahan signifikan pada proyek ETL Pipeline Satu Data Jateng.

Format: [Semantic Versioning](https://semver.org/) — `MAJOR.MINOR.PATCH`

---

## [0.5.0] — 2026-04-18

### Added
- **ColumnNormalizer** (`src/loader/column_normalizer.py`) — Normalisasi nama kolom payload sebelum POST ke API target, mencegah 422 Unprocessable Entity
- **`data/column_mapping.json`** — File konfigurasi terpusat untuk alias kolom (digunakan oleh audit + migrate)
- **Column mapping report** — `data/reports/column_mapping_report.csv` mencatat semua rename yang dilakukan saat migrasi
- 18 unit tests untuk ColumnNormalizer

### Fixed
- **`progress_tracker.py`** — `ValueError: cannot reindex on an axis with duplicate labels` saat upsert berulang kali. Root cause: `loc[mask, k] = v` gagal pada DataFrame dengan duplicate index labels. Fix: drop-then-append pattern
- 2 regression tests ditambahkan untuk bug ini

### Changed
- `MigrationTransformer` sekarang menerima optional `column_normalizer` parameter (backward compatible)
- `MigrationLoadPipeline` menginisialisasi ColumnNormalizer dan menyimpan rename report setelah batch selesai

---

## [0.4.0] — 2026-04-01

### Added
- **DataPreprocessor** (`src/data_preprocessor.py`) — Pipeline preprocessing baru: rename kolom, strip whitespace, fix kode_wilayah
- **`warn_suspicious_year()`** di DataAssessor — Warning non-blocking untuk tahun di luar range
- **`YEAR_MIN` / `YEAR_MAX`** configurable dari `.env`
- **Kode wilayah formatter** — Format BPS otomatis: `3320` → `33.20`, `332001` → `33.20.01`
- 25 unit tests untuk DataPreprocessor

### Fixed
- **`data_preprocessor.py`** — Multiple kolom rename ke target yang sama menyebabkan duplicate columns. Fix: track `used_targets`, skip duplikat
- **`data_preprocessor.py`** — Unicode arrow `→` crash di Windows cp1252 console. Fix: gunakan ASCII `->` di log message

### Changed
- Audit pipeline sekarang menjalankan preprocessing sebelum assessment (urutan: normalize → whitespace → kode_wilayah → assess)

---

## [0.3.0] — 2026-03-31

### Added
- **System Tests** — 11 test untuk audit pipeline E2E + 12 test untuk migrate pipeline E2E
- **Unit Tests** — 40 test baru untuk TargetAPIClient, AutoMapper, MigrationTransformer (total 73 → 96)
- **MigrationProgressTracker** (`src/loader/progress_tracker.py`) — Persistent state tracking antar batch
- **Stateful Migration** — Pipeline migrate sekarang mendukung resume: tabel `done` di-skip, `partial`/`failed` di-retry

### Fixed
- **`loader/client.py`** — `UnboundLocalError` pada `response` saat network timeout. Fix: `response = None` sebelum `try`
- **`loader/transform.py`** — `row.pop("tahun")` memutasi dict asli (side effect). Fix: `row.get("tahun")` + dict comprehension
- **`loader/mapper.py`** — Validasi kolom input (`Dataset_Id`, `Judul_Tabel`) ditambahkan, collision detection diperbaiki

### Changed
- `MigrationLoadPipeline.run()` sepenuhnya di-rewrite menjadi stateful
- README.md di-rewrite total untuk mencerminkan arsitektur baru

---

## [0.2.0] — 2026-03-30

### Added
- **Fuzzy bidirectional row comparison** di CatalogAssessor — Menggantikan MD5 fingerprint yang gagal mendeteksi data hampir identik
- **Routing logic** — Semua baris dari dataset suspect dikirim ke Manager_Review (bukan hanya yang flagged)

### Fixed
- **`catalog_assessor.py`** — MD5 fingerprint menyebabkan false negative pada data yang hampir identik (1 typo). Fix: bidirectional fuzzy matching per baris
- **`catalog_assessor.py`** — `sorted(all_values)` mengubah konteks kolom → false positive. Fix: format `col=val|col=val` yang mempertahankan asosiasi kolom-nilai
- **`pipeline.py`** — Baris ready dari dataset suspect hilang (hanya flagged yang masuk review). Fix: routing SEMUA baris dari dataset suspect/flagged ke review

### Changed
- Threshold duplikasi data disarankan 98% (lebih tinggi dari title threshold 85%)

---

## [0.1.0] — 2026-03-28 (Initial Release)

### Added
- **Mode Audit** — Extract data dari API lama, evaluate, generate report
- **Mode Migrate** — Kirim data ke API target
- **Fuzzy title matching** — Deteksi dataset dengan judul mirip
- **Auto-Mapping** — Mencocokkan ID dataset lama ↔ baru via fuzzy
- **Hybrid Reporting** — Excel (5 sheet) + CSV output
- **CLI interface** — `--mode audit` / `--mode migrate`

### Known Issues (Fixed in later versions)
- `config.py` crash jika `NEW_BASE_URL` env var kosong
- `client.py` typo `cloase()` → `close()`
- `pipeline.py` typo `MigrationTranformer` → `MigrationTransformer`
- `mapper.py` path `data/report/` tanpa 's'
- MD5 fingerprint tidak mendeteksi data hampir identik
- Baris ready dari dataset suspect hilang
