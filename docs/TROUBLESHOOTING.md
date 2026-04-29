# Troubleshooting Guide

Panduan mengatasi error umum yang ditemukan saat menjalankan pipeline ETL.

---

## Error saat Mode Audit

### `BASE_URL belum diset di .env`

**Penyebab:** File `.env` tidak ada atau `BASE_URL` kosong.

**Solusi:**
```bash
cp .env.example .env
# Edit .env, isi BASE_URL dan API_KEY
```

### `No datasets found in catalog. Exiting.`

**Penyebab:** API lama tidak mengembalikan data (server down, token expired, URL salah).

**Solusi:**
1. Cek koneksi internet
2. Verifikasi `BASE_URL` di `.env` bisa diakses via browser/curl
3. Pastikan `API_KEY` masih valid
4. Cek `MAX_PAGES` — minimal 1

### `File column mapping tidak ditemukan`

**Penyebab:** `data/column_mapping.json` tidak ada.

**Efek:** Pipeline tetap jalan, tapi hanya hardcoded tahun patterns yang digunakan untuk rename.

**Solusi:** Buat file dari template:
```json
{
  "column_aliases": {},
  "fuzzy_threshold": 80
}
```

### Output report kosong (0 dataset diproses)

**Penyebab:** `MAX_DATASETS_TO_ASSESS` di `.env` terlalu kecil.

**Solusi:** Naikkan nilainya:
```env
MAX_DATASETS_TO_ASSESS=2000
```

---

## Error saat Mode Migrate

### `422 Client Error: Unprocessable Entity`

**Penyebab:** Nama kolom di payload tidak sesuai yang diharapkan API target.

**Solusi:**
1. Cek `data/reports/failed_payloads_batch*.csv` — lihat kolom apa yang dikirim
2. Cek `data/reports/column_mapping_report.csv` — lihat rename apa yang sudah dilakukan
3. Tambahkan alias baru di `data/column_mapping.json`:
   ```json
   {
     "column_aliases": {
       "nama_kolom_yang_salah": "nama_kolom_yang_benar"
     }
   }
   ```
4. Jalankan ulang — pipeline akan retry tabel yang `failed`/`partial`

### `ValueError: cannot reindex on an axis with duplicate labels`

**Penyebab:** Bug lama di `progress_tracker.py` — sudah diperbaiki di v0.5.0.

**Solusi:**
1. Pastikan kode sudah di-update ke versi terbaru
2. Jika masih terjadi: hapus `data/reports/migration_progress.csv` dan mulai ulang (cold start)

### `Tidak ada kecocokan judul antara data siap load dan katalog yang tersisa`

**Penyebab:**
- Semua tabel yang ada datanya sudah `done` di batch sebelumnya
- Judul dataset di `Load_Ready.csv` tidak cukup mirip dengan judul di katalog target

**Solusi:**
1. Cek `data/reports/migration_progress.csv` — apakah semua sudah `done`?
2. Cek `data/reports/unmapped_datasets.csv` — lihat score kecocokan tertinggi
3. Jika score rendah: turunkan `DUPLICATE_TITLE_THRESHOLD` di `.env` (misal dari 85 ke 75)

### `Gagal mengambil katalog dari sistem baru. Migrasi dibatalkan.`

**Penyebab:** API target tidak bisa dijangkau atau token salah.

**Solusi:**
1. Verifikasi `NEW_BASE_URL` di `.env`
2. Verifikasi `NEW_API_KEY` — pastikan format `Bearer <token>`
3. Test manual: `curl -H "Authorization: Bearer <token>" <NEW_BASE_URL>`

### `File Load Ready tidak ditemukan`

**Penyebab:** Path ke file CSV salah atau belum menjalankan mode audit.

**Solusi:**
```bash
# Jalankan audit dulu
python main.py --mode audit

# Cek output
ls data/reports/Load_Ready_*.csv

# Gunakan path yang benar
python main.py --mode migrate --ready_file data/reports/Load_Ready_XXXXXXXX_XXXX.csv
```

---

## Error saat Testing

### `ModuleNotFoundError: No module named 'src'`

**Penyebab:** Tidak menjalankan pytest dari root directory proyek.

**Solusi:**
```bash
cd ETL_Pipeline_Satu-Data-Jateng
pytest tests/ -v
```

### Test gagal dengan `column_mapping.json` terkait

**Penyebab:** Test mencoba membaca file konfigurasi yang seharusnya di-mock.

**Solusi:** Gunakan `tmp_path` fixture:
```python
def test_something(tmp_path):
    config_path = str(tmp_path / "mapping.json")
    # ... tulis config sementara ...
```

---

## Masalah Umum

### Pipeline berhenti tanpa error jelas

**Cek:** Lihat log file:
```bash
# Windows:
type logs\etl-pipeline.log | findstr /i "error warning"

# Linux:
grep -i "error\|warning" logs/etl-pipeline.log
```

### Progress migrasi hilang setelah restart

**Penyebab:** `migration_progress.csv` terhapus.

**Pencegahan:** **JANGAN** hapus file ini jika ingin melanjutkan migrasi yang terputus. File ini adalah satu-satunya sumber kebenaran untuk status migrasi.

**Recovery:** Jika sudah terhapus, tidak ada cara otomatis untuk recovery. Semua tabel akan dianggap baru dan diproses ulang. Pastikan API target bisa menangani data duplikat (idempotent) sebelum menjalankan ulang.

### Memory tinggi saat memproses banyak dataset

**Solusi:** Kurangi jumlah dataset yang diproses per batch:
```env
MAX_DATASETS_TO_ASSESS=500
```

### Log file terlalu besar

**Solusi:** Rotasi manual:
```bash
# Backup log lama
move logs\etl-pipeline.log logs\etl-pipeline.log.bak

# Pipeline akan membuat file log baru saat dijalankan
```
