# PO Material Cleansing — Web Version
PT Kilang Pertamina · Node.js + Python + PostgreSQL

## Deploy ke Railway

### 1. Push ke GitHub
```bash
git init
git add .
git commit -m "initial"
git remote add origin https://github.com/username/repo.git
git push -u origin main
```

### 2. Buat project di Railway
- New Project → Deploy from GitHub repo
- Pilih repo ini

### 3. Tambah PostgreSQL
- Railway dashboard → New → Database → PostgreSQL
- Link ke project, Railway otomatis set `DATABASE_URL`

### 4. Set Environment Variables
Di Railway project settings → Variables:
```
PORT=3000
PYTHON_PORT=5001
DATABASE_URL=<otomatis dari Railway PostgreSQL>
```

### 5. Deploy
Railway akan auto-build dari Dockerfile dan deploy.

---

## Struktur folder
```
po-cleansing-web/
├── server.js           # Node.js Express (orchestrator)
├── package.json
├── Dockerfile
├── railway.toml
├── requirements.txt    # Python dependencies
├── python/
│   ├── worker.py       # Flask API (Python worker)
│   ├── pipeline_pg.py  # 4-layer pipeline + PostgreSQL cache
│   ├── cleaner.py      # POCleaner facade
│   └── exporter.py     # Excel export
└── public/
    └── index.html      # Frontend (single file)
```

## Arsitektur
- **Node.js**: terima request browser, proxy ke Python worker
- **Python Flask**: jalankan pipeline (Layer 1-4), streaming SSE progress
- **PostgreSQL**: material cache persistent (ganti SQLite)
- **Cache key**: normalized text → tidak perlu proses ulang data yang sama
