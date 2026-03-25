"""
Python Flask worker — versi polling (ganti SSE karena Railway proxy buffer SSE).
Pipeline jalan di background thread, frontend polling /scan-status untuk progress.
"""

import os, sys, json, tempfile, traceback, threading, uuid
from flask import Flask, request, jsonify, send_file
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pipeline_pg import CleansingPipeline, normalize
from cleaner import POCleaner
from exporter import export_results

app = Flask(__name__)
PYTHON_PORT = int(os.environ.get("PYTHON_PORT", 5001))

_sessions  = {}   # session_id -> POCleaner
_jobs      = {}   # job_id -> {status, pct, msg, result, error}
_jobs_lock = threading.Lock()


def _get_cleaner(sid):
    if sid not in _sessions:
        _sessions[sid] = POCleaner()
    return _sessions[sid]


@app.route("/health")
def health():
    return jsonify({"ok": True})


@app.route("/upload", methods=["POST"])
def upload():
    sid = request.form.get("session_id", "default")
    if "file" not in request.files:
        return jsonify({"ok": False, "error": "No file"}), 400
    f = request.files["file"]
    tmp = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
    f.save(tmp.name); tmp.close()
    cleaner = _get_cleaner(sid)
    result  = cleaner.load_excel(tmp.name)
    os.unlink(tmp.name)
    if not result["ok"]:
        return jsonify(result), 400
    return jsonify(result)


@app.route("/set-column", methods=["POST"])
def set_column():
    d = request.json or {}
    cleaner = _get_cleaner(d.get("session_id","default"))
    return jsonify(cleaner.set_desc_column(d.get("column","")))


@app.route("/scan-start", methods=["POST"])
def scan_start():
    """Mulai pipeline di background, return job_id."""
    d         = request.json or {}
    sid       = d.get("session_id","default")
    api_key   = d.get("api_key","")
    skip_llm  = d.get("skip_llm", True)
    threshold = int(d.get("threshold", 75))
    job_id    = str(uuid.uuid4())

    with _jobs_lock:
        _jobs[job_id] = {"status":"running","pct":0,"msg":"Memulai...","result":None,"error":None}

    def run():
        cleaner = _get_cleaner(sid)
        def prog(pct, msg):
            with _jobs_lock:
                if job_id in _jobs:
                    _jobs[job_id]["pct"] = round(pct * 100)
                    _jobs[job_id]["msg"] = msg
        try:
            api_config = {
                "base_url": "https://ai.dinoiki.com/v1",
                "api_key":  api_key,
                "model":    "claude-sonnet-4-6",
            } if api_key else {}

            cleaner.run_pipeline(
                api_config=api_config,
                threshold=threshold,
                progress_cb=prog,
                skip_llm=skip_llm or not api_key,
            )
            s = cleaner.stats()
            clusters_data = [_serialize_cluster(c) for c in cleaner.clusters]
            with _jobs_lock:
                _jobs[job_id] = {
                    "status": "done", "pct": 100, "msg": "Selesai.",
                    "result": {"stats": s, "clusters": clusters_data},
                    "error": None
                }
        except Exception as e:
            with _jobs_lock:
                _jobs[job_id] = {
                    "status": "error", "pct": 0,
                    "msg": str(e), "result": None,
                    "error": traceback.format_exc()
                }

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"ok": True, "job_id": job_id})


@app.route("/scan-status")
def scan_status():
    """Poll progress job."""
    job_id = request.args.get("job_id","")
    with _jobs_lock:
        job = _jobs.get(job_id)
    if not job:
        return jsonify({"status":"not_found"}), 404
    return jsonify(job)


@app.route("/export", methods=["POST"])
def export():
    d         = request.json or {}
    sid       = d.get("session_id","default")
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
        result = export_results(final_rows, audit_entries, tmp.name, cleaner.header_row, s)
        if not result["ok"]:
            return jsonify(result), 500
        return send_file(
            tmp.name, as_attachment=True,
            download_name="PO_Clean_Result.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/cache-stats")
def cache_stats():
    sid = request.args.get("session_id","default")
    try:
        return jsonify(_get_cleaner(sid).cache_stats())
    except Exception:
        return jsonify({"total": 0})


def _serialize_cluster(c):
    return {
        "id":           c["id"],
        "cluster_id":   c.get("cluster_id",""),
        "orig_descs":   c.get("orig_descs",[]),
        "suggested_name": c.get("suggested_name",""),
        "avg_score":    c.get("avg_score",0),
        "is_duplicate": c.get("is_duplicate",False),
        "is_auto":      c.get("is_auto",False),
        "method":       c.get("method",""),
        "confidence":   c.get("confidence",""),
        "ai_result":    c.get("ai_result"),
        "items_count":  len(c.get("items",[])),
        "ru_list":      sorted({
            str(r.get("Sumber RU",""))
            for r in c.get("items",[]) if r.get("Sumber RU")
        }),
    }


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PYTHON_PORT, debug=False)
