"""
Python Flask worker — dipanggil oleh Node.js via HTTP.
Menjalankan pipeline 4-layer dan streaming progress via SSE.
Database cache: PostgreSQL (bukan SQLite).
"""

import os, sys, json, tempfile, traceback
from flask import Flask, request, jsonify, Response, stream_with_context, send_file
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Patch pipeline untuk pakai PostgreSQL sebagai cache
from pipeline_pg import CleansingPipeline, normalize
from cleaner import POCleaner
from exporter import export_results

app = Flask(__name__)
PYTHON_PORT = int(os.environ.get("PYTHON_PORT", 5001))

# ── Satu session per upload (simpel, cukup untuk use case ini) ───────
_sessions = {}   # session_id -> POCleaner instance


def _get_cleaner(session_id: str) -> POCleaner:
    if session_id not in _sessions:
        _sessions[session_id] = POCleaner()
    return _sessions[session_id]


# ─────────────────────────────────────────────────────────────────────
@app.route("/health")
def health():
    return jsonify({"ok": True})


@app.route("/upload", methods=["POST"])
def upload():
    """Terima file Excel, simpan sementara, load ke cleaner."""
    session_id = request.form.get("session_id", "default")
    if "file" not in request.files:
        return jsonify({"ok": False, "error": "No file"}), 400

    f = request.files["file"]
    tmp = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
    f.save(tmp.name)
    tmp.close()

    cleaner = _get_cleaner(session_id)
    result = cleaner.load_excel(tmp.name)
    os.unlink(tmp.name)

    if not result["ok"]:
        return jsonify(result), 400
    return jsonify(result)


@app.route("/set-column", methods=["POST"])
def set_column():
    data = request.json or {}
    session_id = data.get("session_id", "default")
    col = data.get("column", "")
    cleaner = _get_cleaner(session_id)
    result = cleaner.set_desc_column(col)
    return jsonify(result)


@app.route("/scan-stream")
def scan_stream():
    """SSE: jalankan pipeline, stream progress ke client."""
    session_id = request.args.get("session_id", "default")
    api_key    = request.args.get("api_key", "")
    skip_llm   = request.args.get("skip_llm", "true").lower() == "true"
    threshold  = int(request.args.get("threshold", 75))

    cleaner = _get_cleaner(session_id)

    def generate():
        def progress_cb(pct, msg):
            yield f"data: {json.dumps({'type':'progress','pct':round(pct*100),'msg':msg})}\n\n"

        # SSE tidak bisa yield dari callback — pakai queue
        import queue, threading
        q = queue.Queue()

        def prog(pct, msg):
            q.put({"type": "progress", "pct": round(pct * 100), "msg": msg})

        def run():
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
                q.put({
                    "type": "done",
                    "stats": s,
                    "clusters": clusters_data,
                })
            except Exception as e:
                q.put({"type": "error", "msg": str(e), "trace": traceback.format_exc()})

        t = threading.Thread(target=run, daemon=True)
        t.start()

        while True:
            try:
                item = q.get(timeout=120)
                yield f"data: {json.dumps(item)}\n\n"
                if item["type"] in ("done", "error"):
                    break
            except Exception:
                yield f"data: {json.dumps({'type':'error','msg':'timeout'})}\n\n"
                break

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        }
    )


@app.route("/export", methods=["POST"])
def export():
    data = request.json or {}
    session_id = data.get("session_id", "default")
    decisions  = data.get("decisions", {})  # {cluster_id: {action, canonical_name}}

    cleaner = _get_cleaner(session_id)

    # Apply decisions dari frontend
    for cid_str, dec in decisions.items():
        try:
            cid = int(cid_str)
            cleaner.save_decision(cid, dec["action"], dec["canonical_name"])
        except Exception:
            pass

    tmp = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
    tmp.close()

    try:
        final_rows, audit_entries = cleaner.apply_decisions()
        s = cleaner.stats()
        s["source_file"] = "web_upload.xlsx"
        s["threshold"]   = 75
        result = export_results(final_rows, audit_entries, tmp.name,
                                cleaner.header_row, s)
        if not result["ok"]:
            return jsonify(result), 500

        return send_file(
            tmp.name,
            as_attachment=True,
            download_name="PO_Clean_Result.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/cache-stats")
def cache_stats():
    session_id = request.args.get("session_id", "default")
    cleaner = _get_cleaner(session_id)
    try:
        return jsonify(cleaner.cache_stats())
    except Exception:
        return jsonify({"total": 0})


def _serialize_cluster(c: dict) -> dict:
    """Buat cluster JSON-safe untuk dikirim ke frontend."""
    return {
        "id":          c["id"],
        "cluster_id":  c.get("cluster_id", ""),
        "orig_descs":  c.get("orig_descs", []),
        "suggested_name": c.get("suggested_name", ""),
        "avg_score":   c.get("avg_score", 0),
        "is_duplicate": c.get("is_duplicate", False),
        "is_auto":     c.get("is_auto", False),
        "method":      c.get("method", ""),
        "confidence":  c.get("confidence", ""),
        "ai_result":   c.get("ai_result"),
        "items_count": len(c.get("items", [])),
        "ru_list":     sorted({
            str(r.get("Sumber RU", ""))
            for r in c.get("items", [])
            if r.get("Sumber RU")
        }),
    }


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PYTHON_PORT, debug=False)
