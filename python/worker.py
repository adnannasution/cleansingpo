"""
Python Flask worker — job store pakai PostgreSQL bukan in-memory dict.
Fix: _jobs={} in-memory tidak shared antar Gunicorn workers → job hilang saat poll.
"""

import os, sys, json, tempfile, traceback, threading, uuid
from flask import Flask, request, jsonify, send_file
import psycopg2
from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pipeline_pg import CleansingPipeline, normalize
from cleaner import POCleaner
from exporter import export_results

app = Flask(__name__)
PYTHON_PORT = int(os.environ.get("PYTHON_PORT", 5001))
DATABASE_URL = os.environ.get("DATABASE_URL", "")

# ── Session store tetap in-memory (per-worker OK karena sticky session) ──
_sessions     = {}
_sessions_lock = threading.Lock()


# ════════════════════════════════════════════════════════════════════
# PostgreSQL job store  (ganti _jobs = {} yang tidak shared)
# ════════════════════════════════════════════════════════════════════

def _pg_conn():
    url = DATABASE_URL
    # Railway kadang pakai postgres:// — psycopg2 butuh postgresql://
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(url, cursor_factory=RealDictCursor)


def _init_jobs_table():
    """Buat tabel jobs kalau belum ada. Dipanggil sekali saat startup."""
    try:
        con = _pg_conn()
        with con.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS scan_jobs (
                    job_id    TEXT PRIMARY KEY,
                    status    TEXT NOT NULL DEFAULT 'running',
                    pct       INTEGER DEFAULT 0,
                    msg       TEXT DEFAULT '',
                    result    TEXT,
                    error     TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            # Hapus job lama >1 jam supaya tabel tidak membengkak
            cur.execute("""
                DELETE FROM scan_jobs
                WHERE created_at < NOW() - INTERVAL '1 hour'
            """)
        con.commit()
        con.close()
        print("[PG] scan_jobs table ready.")
    except Exception as e:
        print(f"[PG] WARNING: tidak bisa init tabel jobs: {e}")


def _job_create(job_id: str):
    try:
        con = _pg_conn()
        with con.cursor() as cur:
            cur.execute(
                "INSERT INTO scan_jobs (job_id, status, pct, msg) "
                "VALUES (%s, 'running', 0, 'Memulai...')",
                (job_id,)
            )
        con.commit(); con.close()
    except Exception as e:
        print(f"[PG] job_create error: {e}")


def _job_update(job_id: str, pct: int, msg: str):
    try:
        con = _pg_conn()
        with con.cursor() as cur:
            cur.execute(
                "UPDATE scan_jobs SET pct=%s, msg=%s WHERE job_id=%s",
                (pct, msg, job_id)
            )
        con.commit(); con.close()
    except Exception as e:
        print(f"[PG] job_update error: {e}")


def _job_done(job_id: str, result_dict: dict):
    try:
        con = _pg_conn()
        with con.cursor() as cur:
            cur.execute(
                "UPDATE scan_jobs SET status='done', pct=100, msg='Selesai.', "
                "result=%s WHERE job_id=%s",
                (json.dumps(result_dict, default=str), job_id)
            )
        con.commit(); con.close()
    except Exception as e:
        print(f"[PG] job_done error: {e}")


def _job_error(job_id: str, msg: str, tb: str):
    try:
        con = _pg_conn()
        with con.cursor() as cur:
            cur.execute(
                "UPDATE scan_jobs SET status='error', msg=%s, error=%s "
                "WHERE job_id=%s",
                (msg, tb, job_id)
            )
        con.commit(); con.close()
    except Exception as e:
        print(f"[PG] job_error error: {e}")


def _job_get(job_id: str) -> dict | None:
    try:
        con = _pg_conn()
        with con.cursor() as cur:
            cur.execute("SELECT * FROM scan_jobs WHERE job_id=%s", (job_id,))
            row = cur.fetchone()
        con.close()
        if not row:
            return None
        result = dict(row)
        # Parse result JSON kalau ada
        if result.get("result") and isinstance(result["result"], str):
            try:
                result["result"] = json.loads(result["result"])
            except Exception:
                pass
        return result
    except Exception as e:
        print(f"[PG] job_get error: {e}")
        return None


# ════════════════════════════════════════════════════════════════════
# Session helpers
# ════════════════════════════════════════════════════════════════════

def _get_cleaner(sid: str) -> POCleaner:
    with _sessions_lock:
        if sid not in _sessions:
            _sessions[sid] = POCleaner()
        return _sessions[sid]


# ════════════════════════════════════════════════════════════════════
# Routes
# ════════════════════════════════════════════════════════════════════

@app.route("/health")
def health():
    return jsonify({"ok": True})


@app.route("/upload", methods=["POST"])
def upload():
    sid = request.form.get("session_id", "default")
    if "file" not in request.files:
        return jsonify({"ok": False, "error": "No file"}), 400
    f   = request.files["file"]
    tmp = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
    f.save(tmp.name); tmp.close()
    cleaner = _get_cleaner(sid)
    result  = cleaner.load_excel(tmp.name)
    try:
        os.unlink(tmp.name)
    except Exception:
        pass
    if not result["ok"]:
        return jsonify(result), 400
    return jsonify(result)


@app.route("/set-column", methods=["POST"])
def set_column():
    d       = request.json or {}
    cleaner = _get_cleaner(d.get("session_id", "default"))
    return jsonify(cleaner.set_desc_column(d.get("column", "")))


@app.route("/scan-start", methods=["POST"])
def scan_start():
    """Mulai pipeline di background thread, return job_id."""
    d         = request.json or {}
    sid       = d.get("session_id", "default")
    api_key   = d.get("api_key", "")
    skip_llm  = d.get("skip_llm", True)
    threshold = int(d.get("threshold", 75))
    job_id    = str(uuid.uuid4())

    # Simpan job ke PostgreSQL — visible ke SEMUA worker
    _job_create(job_id)

    def run():
        cleaner = _get_cleaner(sid)

        def prog(pct_float, msg):
            _job_update(job_id, round(pct_float * 100), msg)

        try:
            api_config = {
                "base_url": "https://ai.dinoiki.com/v1",
                "api_key":  api_key,
                "model":    "claude-sonnet-4-6",
            } if api_key else {}

            cleaner.run_pipeline(
                api_config  = api_config,
                threshold   = threshold,
                progress_cb = prog,
                skip_llm    = skip_llm or not api_key,
            )

            clusters_data = [_serialize_cluster(c) for c in cleaner.clusters]
            _job_done(job_id, {
                "stats":    cleaner.stats(),
                "clusters": clusters_data,
            })

        except Exception as e:
            _job_error(job_id, str(e), traceback.format_exc())
            print(f"[Pipeline ERROR] {traceback.format_exc()}")

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"ok": True, "job_id": job_id})


@app.route("/scan-status")
def scan_status():
    """Poll progress — baca dari PostgreSQL, bukan in-memory."""
    job_id = request.args.get("job_id", "")
    job    = _job_get(job_id)
    if not job:
        return jsonify({"status": "not_found"}), 404

    return jsonify({
        "status": job["status"],
        "pct":    job.get("pct", 0),
        "msg":    job.get("msg", ""),
        "result": job.get("result"),
        "error":  job.get("error"),
    })


@app.route("/export", methods=["POST"])
def export():
    d         = request.json or {}
    sid       = d.get("session_id", "default")
    decisions = d.get("decisions", {})
    cleaner   = _get_cleaner(sid)

    for cid_str, dec in decisions.items():
        try:
            cleaner.save_decision(int(cid_str), dec["action"], dec["canonical_name"])
        except Exception:
            pass

    tmp = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
    tmp.close()
    try:
        final_rows, audit_entries = cleaner.apply_decisions()
        s = cleaner.stats()
        s["source_file"] = "web_upload.xlsx"
        s["threshold"]   = 75
        result = export_results(
            final_rows, audit_entries, tmp.name, cleaner.header_row, s)
        if not result["ok"]:
            return jsonify(result), 500
        return send_file(
            tmp.name, as_attachment=True,
            download_name="PO_Clean_Result.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/cache-stats")
def cache_stats():
    sid = request.args.get("session_id", "default")
    try:
        return jsonify(_get_cleaner(sid).cache_stats())
    except Exception:
        return jsonify({"total": 0})


# ════════════════════════════════════════════════════════════════════
# Helpers
# ════════════════════════════════════════════════════════════════════

def _serialize_cluster(c: dict) -> dict:
    return {
        "id":             c["id"],
        "cluster_id":     c.get("cluster_id", ""),
        "orig_descs":     c.get("orig_descs", []),
        "suggested_name": c.get("suggested_name", ""),
        "avg_score":      c.get("avg_score", 0),
        "is_duplicate":   c.get("is_duplicate", False),
        "is_auto":        c.get("is_auto", False),
        "method":         c.get("method", ""),
        "confidence":     c.get("confidence", ""),
        "ai_result":      c.get("ai_result"),
        "items_count":    len(c.get("items", [])),
        "ru_list":        sorted({
            str(r.get("Sumber RU", ""))
            for r in c.get("items", [])
            if r.get("Sumber RU")
        }),
    }


# ════════════════════════════════════════════════════════════════════
# Startup
# ════════════════════════════════════════════════════════════════════

# Init tabel jobs saat module di-load (works di Gunicorn multi-worker)
if DATABASE_URL:
    _init_jobs_table()
else:
    print("[WARNING] DATABASE_URL tidak di-set — job store tidak akan berfungsi di multi-worker!")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PYTHON_PORT, debug=False)