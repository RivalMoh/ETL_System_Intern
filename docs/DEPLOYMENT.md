# Panduan Operasional (Deployment)

Panduan step-by-step untuk menjalankan pipeline ETL di production.

---

## Prasyarat

1. Python 3.10+ terinstal
2. Virtual environment sudah di-setup (lihat [DEVELOPER_GUIDE.md](DEVELOPER_GUIDE.md))
3. File `.env` sudah diisi dengan kredensial yang benar
4. Koneksi internet ke API lama dan API target

---

## Workflow Operasional

```
                    ┌──────────────┐
                    │  1. AUDIT    │
                    │  (otomatis)  │
                    └──────┬───────┘
                           │
                    ┌──────▼───────┐
                    │  2. REVIEW   │
                    │  (manual)    │
                    └──────┬───────┘
                           │
                    ┌──────▼───────┐
           ┌──────►│  3. MIGRATE  │◄──────┐
           │       │  (otomatis)  │       │
           │       └──────┬───────┘       │
           │              │               │
           │       ┌──────▼───────┐       │
           │       │  4. MONITOR  │       │
           │       │  (manual)    │       │
           │       └──────┬───────┘       │
           │              │               │
           │    ┌─────────┼─────────┐     │
           │    │ Semua    │ Ada     │     │
           │    │ done?    │ sisa?   │     │
           │    ▼         ▼         │     │
           │  SELESAI   Ulangi ─────┘     │
           │             langkah 3        │
           │                              │
           └── Jika ada error 422 ────────┘
               fix column_mapping.json
               lalu ulangi langkah 3
```

---

## Langkah 1: Jalankan Mode Audit

```bash
# Aktifkan virtual environment
.venv\Scripts\activate        # Windows
source .venv/bin/activate     # Linux/Mac

# Jalankan audit
python main.py --mode audit
```

**Durasi:** Tergantung jumlah dataset. ~1000 dataset ≈ 10-30 menit.

**Output yang dihasilkan:**

| File | Lokasi | Deskripsi |
|---|---|---|
| Audit Report | `data/reports/Audit_Migrasi_*.xlsx` | Laporan eksekutif (5 sheet) |
| Load Ready | `data/reports/Load_Ready_*.csv` | Data bersih, siap migrasi |
| Manager Review | `data/reports/Manager_Review_*.csv` | Data perlu review manual |

---

## Langkah 2: Review Manual

1. Buka `Audit_Migrasi_*.xlsx` di Excel
2. Cek sheet **"Ringkasan Load"** — berapa persen data siap load?
3. Cek sheet **"Data Duplikat"** — ada dataset duplikat?
4. Buka `Manager_Review_*.csv` — review data yang di-flag:
   - `migration_status = flagged` → data rusak (missing values)
   - `Catalog_Suspect = True` → kemungkinan duplikat
5. **Keputusan:**
   - Jika data sudah OK → lanjut ke langkah 3
   - Jika ada masalah → perbaiki data di sumber, lalu ulangi langkah 1

---

## Langkah 3: Jalankan Mode Migrate

```bash
python main.py --mode migrate --ready_file data/reports/Load_Ready_XXXXXXXX_XXXX.csv
```

**Catatan:**
- Ganti `XXXXXXXX_XXXX` dengan timestamp yang sesuai
- Pipeline **stateful** — aman untuk dijalankan berulang kali
- Tabel yang sudah `done` akan otomatis di-skip

---

## Langkah 4: Monitoring Progress

### Cek Status via Log

```bash
# Lihat ringkasan batch terakhir
type logs\etl-pipeline.log | findstr "BATCH"

# Contoh output:
# === BATCH #3 SELESAI | Done: 5 | Partial: 1 | Gagal: 0 ===
# PROGRESS TOTAL: 48/50 tabel selesai | 2 tabel tersisa
```

### Cek Status via Progress File

```bash
# Buka di Excel/editor
data/reports/migration_progress.csv
```

Kolom penting:
| Kolom | Arti |
|---|---|
| `status` | `done` / `partial` / `failed` |
| `rows_sent` | Jumlah baris yang berhasil dikirim |
| `batch_number` | Batch ke berapa |
| `migrated_at` | Timestamp terakhir diproses |

### Cek Kolom yang Di-rename

```bash
# Lihat rename report
data/reports/column_mapping_report.csv
```

### Cek Payload yang Gagal

```bash
# Per batch
data/reports/failed_payloads_batch1.csv
data/reports/failed_payloads_batch2.csv
```

---

## Menangani Error 422

Jika ada tabel yang gagal dengan `422 Unprocessable Entity`:

1. **Identifikasi kolom yang salah:**
   ```bash
   # Lihat payload yang gagal
   type data\reports\failed_payloads_batch*.csv
   ```

2. **Tambah alias di `column_mapping.json`:**
   ```json
   {
     "column_aliases": {
       "nama_kolom_salah": "nama_kolom_benar"
     }
   }
   ```

3. **Jalankan ulang migrate:**
   ```bash
   python main.py --mode migrate --ready_file data/reports/Load_Ready_*.csv
   ```
   Tabel yang `failed`/`partial` akan otomatis diretry.

---

## Menjalankan Ulang dari Awal (Reset)

Jika perlu reset total:

```bash
# Hapus progress file
del data\reports\migration_progress.csv

# Jalankan ulang
python main.py --mode migrate --ready_file data/reports/Load_Ready_*.csv
```

> **PERINGATAN:** Ini akan memproses SEMUA tabel dari awal. Pastikan API target bisa menangani data duplikat (idempotent), atau bersihkan data di target terlebih dahulu.

---

## Checklist Production

Sebelum menjalankan di production, pastikan:

- [ ] `.env` terisi lengkap (semua URL dan API key)
- [ ] `column_mapping.json` sudah diisi alias yang sesuai
- [ ] Koneksi ke API lama dan target sudah diverifikasi
- [ ] Mode audit sudah dijalankan dan report sudah di-review
- [ ] `Load_Ready_*.csv` sudah ada dan tidak kosong
- [ ] Backup `migration_progress.csv` jika ada (untuk recovery)
- [ ] Tim sudah diinformasikan bahwa migrasi akan berjalan
- [ ] Monitoring log file sudah disiapkan

---

## Environment Variables Reference

| Variable | Wajib | Mode | Deskripsi |
|---|---|---|---|
| `BASE_URL` | ✅ | Audit | URL API sistem lama |
| `API_KEY` | ✅ | Audit | Token autentikasi sistem lama |
| `MAX_PAGES` | ❌ | Audit | Maks halaman katalog (default: 1) |
| `MAX_DATASETS_TO_ASSESS` | ❌ | Audit | Maks dataset yang dievaluasi (default: 5) |
| `DUPLICATE_TITLE_THRESHOLD` | ❌ | Both | Threshold kemiripan judul 0-100 (default: 85) |
| `DUPLICATE_SAMPLE_SIZE` | ❌ | Audit | Baris sampel untuk cek duplikat (default: 5) |
| `REQUIRED_COLUMNS` | ❌ | Audit | Kolom wajib, comma-separated (default: tahun,jumlah) |
| `LOAD_ALLOWED_STATUSES` | ❌ | Audit | Status yang boleh dimuat (default: ready) |
| `YEAR_MIN` | ❌ | Audit | Batas bawah tahun untuk warning (default: 2000) |
| `YEAR_MAX` | ❌ | Audit | Batas atas tahun untuk warning (default: 2025) |
| `NEW_BASE_URL` | ✅ | Migrate | URL API sistem target |
| `NEW_API_KEY` | ✅ | Migrate | Token autentikasi sistem target |
