"""
pipeline_pg.py — pipeline.py dengan cache PostgreSQL menggantikan SQLite.
Hanya mengganti class MaterialCache, semua logic Layer 1-4 tetap sama.
"""

import os, re, json, threading
from datetime import datetime
from typing import Callable, Optional
from collections import defaultdict
from rapidfuzz import fuzz, process as rf_process

# ── PostgreSQL ────────────────────────────────────────────────────── #
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    HAS_PG = True
except ImportError:
    HAS_PG = False

DATABASE_URL = os.environ.get("DATABASE_URL", "")

# ── Normalisasi (Layer 1) — sama persis dengan pipeline.py ────────── #
_NORM_RULES = [
    (r'\bINCHES\b','IN'),(r'\bINCH\b','IN'),(r'(?<=\d)\s*"','IN'),
    (r"(?<=\d)\s*'",'IN'),(r'\bMILIMETER(S)?\b','MM'),(r'\bMILI\b','MM'),
    (r'\bCENTIMETER(S)?\b','CM'),(r'\bMETER(S)?\b','M'),
    (r'\bPIECES\b','PCS'),(r'\bPIECE\b','PCS'),(r'\bPC\b','PCS'),
    (r'\bUNITS\b','UNIT'),(r'\bPOUND(S)?\b','LBS'),(r'\bLB\b','LBS'),
    (r'\bSCH\s*[-\.]?\s*(\d+)\b',r'SCH\1'),(r'\bCARBON\s*STEEL\b','CS'),
    (r'\bKARBON\b','CARBON'),(r'\bBAJA\b','STEEL'),(r'\bBOLA\b','BALL'),
    (r'\bKATUP\b','VALVE'),(r'\bPOMPA\b','PUMP'),(r'\bKOPLING\b','COUPLING'),
    (r'\bBANTALAN\b','BEARING'),(r'\bSARINGAN\b','FILTER'),
    (r'\bMINYAK\b','OIL'),(r'\bPELUMAS\b','LUBRICANT'),(r'\bOLI\b','OIL'),
    (r'\bPIPA\b','PIPE'),(r'\bKABEL\b','CABLE'),(r'\bINSULASI\b','INSULATION'),
    (r'\bMANOMETER\b','PRESSURE GAUGE'),(r'\bALAT UKUR TEKANAN\b','PRESSURE GAUGE'),
    (r'\bPERAPAT\b','SEAL'),(r'\bPACKING\b','GASKET'),
    (r'[_\-\/\\]+',' '),(r'\s{2,}',' '),
]
_COMPILED = [(re.compile(p, re.IGNORECASE), r) for p, r in _NORM_RULES]

def normalize(text: str) -> str:
    text = str(text).upper().strip()
    text = re.sub(r'^[^A-Z0-9]+|[^A-Z0-9]+$', '', text)
    for pat, repl in _COMPILED:
        text = pat.sub(repl, text)
    return text.strip()


# ── Layer 2: PostgreSQL Cache ─────────────────────────────────────── #
_CACHE_LOCK = threading.Lock()

class MaterialCache:
    def __init__(self):
        self._local = {}   # fallback in-memory jika PG tidak tersedia
        if HAS_PG and DATABASE_URL:
            self._init_pg()
        else:
            print("[Cache] PostgreSQL tidak tersedia, pakai in-memory cache.")

    def _connect(self):
        return psycopg2.connect(DATABASE_URL, sslmode="require")

    def _init_pg(self):
        with _CACHE_LOCK:
            try:
                con = self._connect()
                cur = con.cursor()
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS material_cache (
                        normalized_key  TEXT PRIMARY KEY,
                        original_text   TEXT,
                        cleaned_text    TEXT NOT NULL,
                        method          TEXT NOT NULL,
                        cluster_id      TEXT,
                        confidence      TEXT,
                        reasoning       TEXT,
                        created_at      TIMESTAMPTZ DEFAULT NOW(),
                        hit_count       INTEGER DEFAULT 1
                    )
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_mat_cache_key
                    ON material_cache(normalized_key)
                """)
                con.commit(); cur.close(); con.close()
                print("[Cache] PostgreSQL terhubung & tabel siap.")
            except Exception as e:
                print(f"[Cache] PostgreSQL error: {e}")

    def get(self, key: str) -> Optional[dict]:
        if HAS_PG and DATABASE_URL:
            try:
                with _CACHE_LOCK:
                    con = self._connect()
                    cur = con.cursor(cursor_factory=RealDictCursor)
                    cur.execute(
                        "SELECT cleaned_text,method,cluster_id,confidence,reasoning "
                        "FROM material_cache WHERE normalized_key=%s", (key,))
                    row = cur.fetchone()
                    if row:
                        cur.execute(
                            "UPDATE material_cache SET hit_count=hit_count+1 "
                            "WHERE normalized_key=%s", (key,))
                    con.commit(); cur.close(); con.close()
                if row:
                    return dict(row)
            except Exception:
                pass
        return self._local.get(key)

    def set(self, key: str, original: str, cleaned: str, method: str,
            cluster_id: str = None, confidence: str = None, reasoning: str = None):
        entry = {"cleaned_text": cleaned, "method": method,
                 "cluster_id": cluster_id, "confidence": confidence,
                 "reasoning": reasoning}
        self._local[key] = entry
        if HAS_PG and DATABASE_URL:
            try:
                with _CACHE_LOCK:
                    con = self._connect()
                    cur = con.cursor()
                    cur.execute("""
                        INSERT INTO material_cache
                            (normalized_key,original_text,cleaned_text,method,
                             cluster_id,confidence,reasoning)
                        VALUES (%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (normalized_key) DO UPDATE SET
                            cleaned_text=EXCLUDED.cleaned_text,
                            method=EXCLUDED.method,
                            cluster_id=EXCLUDED.cluster_id,
                            confidence=EXCLUDED.confidence,
                            reasoning=EXCLUDED.reasoning,
                            hit_count=material_cache.hit_count+1
                    """, (key, original, cleaned, method, cluster_id, confidence, reasoning))
                    con.commit(); cur.close(); con.close()
            except Exception as e:
                print(f"[Cache.set] {e}")

    def bulk_get(self, keys: list) -> dict:
        if not keys:
            return {}
        result = {}
        if HAS_PG and DATABASE_URL:
            try:
                with _CACHE_LOCK:
                    con = self._connect()
                    cur = con.cursor(cursor_factory=RealDictCursor)
                    cur.execute(
                        "SELECT normalized_key,cleaned_text,method,cluster_id,confidence,reasoning "
                        "FROM material_cache WHERE normalized_key=ANY(%s)", (keys,))
                    for row in cur.fetchall():
                        result[row["normalized_key"]] = dict(row)
                    cur.close(); con.close()
                return result
            except Exception:
                pass
        return {k: self._local[k] for k in keys if k in self._local}

    def stats(self) -> dict:
        if HAS_PG and DATABASE_URL:
            try:
                with _CACHE_LOCK:
                    con = self._connect()
                    cur = con.cursor()
                    cur.execute("SELECT COUNT(*) FROM material_cache")
                    total = cur.fetchone()[0]
                    cur.execute("SELECT method, COUNT(*) FROM material_cache GROUP BY method")
                    by_method = dict(cur.fetchall())
                    cur.close(); con.close()
                return {"total": total, "by_method": by_method}
            except Exception:
                pass
        return {"total": len(self._local), "by_method": {}}


# ── Layer 3 constants ─────────────────────────────────────────────── #
AUTO_MERGE_THRESHOLD = 95
CLUSTER_THRESHOLD_HI = 90
CLUSTER_THRESHOLD_LO = 75
BATCH_SIZE = 10

_SYSTEM_PROMPT = """Kamu adalah expert material engineer di industri kilang minyak (refinery).
Analisis apakah deskripsi material berikut merujuk ke barang yang SAMA atau BERBEDA.
- Perbedaan bahasa/kapitalisasi/singkatan BUKAN berarti beda barang
- Perbedaan spesifikasi teknis (ukuran, rating, schedule, grade) BERARTI beda barang
Respond ONLY dengan JSON array valid."""


def _rule_based(a: str, b: str) -> bool:
    def strip(s): return re.sub(r'[\s\-_xX×]', '', s)
    return strip(a) == strip(b)


def build_clusters(unique_descs: list, cache: MaterialCache,
                   progress_cb: Callable = None) -> tuple:
    total = len(unique_descs)
    norm_map = {d: normalize(d) for d in unique_descs}
    unique_norms = list(dict.fromkeys(norm_map.values()))

    # Layer 2: bulk cache lookup
    cache_hits = {}
    cached = cache.bulk_get(unique_norms)
    for d in unique_descs:
        nk = norm_map[d]
        if nk in cached:
            cache_hits[nk] = cached[nk]
    if progress_cb:
        progress_cb(0.08, f"Layer 2: {len(cache_hits)} dari {total} hit cache")

    pending = [d for d in unique_descs if norm_map[d] not in cache_hits]
    pending_norms = list(dict.fromkeys(norm_map[d] for d in pending))

    # Layer 3: fuzzy clustering
    auto_merged = {}
    clusters = []
    visited = set()

    for i, nk in enumerate(pending_norms):
        if progress_cb and i % 20 == 0:
            progress_cb(0.10 + 0.50 * (i / max(len(pending_norms), 1)),
                        f"Layer 3: fuzzy {i+1}/{len(pending_norms)}")
        if nk in visited:
            continue

        matches = rf_process.extract(
            nk, pending_norms, scorer=fuzz.token_sort_ratio,
            score_cutoff=CLUSTER_THRESHOLD_LO)
        group_nks = [m[0] for m in matches]
        group_scores = {m[0]: m[1] for m in matches}
        visited.update(group_nks)

        orig_descs = [d for d in pending if norm_map[d] in group_nks]
        if not orig_descs:
            continue

        best_score = max(group_scores.values()) if group_scores else 100
        from collections import Counter
        freq = Counter(str(d) for d in orig_descs)
        suggested = freq.most_common(1)[0][0]
        min_score = min(group_scores.values()) if group_scores else 100

        is_auto = min_score >= AUTO_MERGE_THRESHOLD
        is_dup  = len(set(orig_descs)) > 1

        cid = f"C{abs(hash(nk)) % 99999:05d}"

        if is_auto:
            for gnk in group_nks:
                auto_merged[gnk] = {
                    "cleaned_text": suggested, "method": "FUZZY_AUTO",
                    "cluster_id": cid, "confidence": "high",
                }
                cache.set(gnk, suggested, suggested, "FUZZY_AUTO", cid, "high")

        clusters.append({
            "cluster_id": cid, "norm_keys": group_nks,
            "orig_descs": orig_descs, "suggested_name": suggested,
            "min_score": min_score, "is_duplicate": is_dup,
            "is_auto": is_auto,
        })

    pending_ai = [c for c in clusters
                  if not c["is_auto"] and c["is_duplicate"]
                  and CLUSTER_THRESHOLD_LO <= c["min_score"] < AUTO_MERGE_THRESHOLD]

    if progress_cb:
        progress_cb(0.65, f"Layer 3 selesai. {len(pending_ai)} cluster untuk AI.")

    layer_stats = {
        "cache_hits":  len(cache_hits),
        "auto_merged": len(auto_merged),
        "total_unique": total,
    }
    return clusters, auto_merged, cache_hits, pending_ai, layer_stats


def validate_batch(clusters_batch: list, api_config: dict) -> list:
    import urllib.request, urllib.error
    items = [{"cluster_id": cl["cluster_id"],
              "variants": cl["orig_descs"][:6],
              "suggested_name": cl["suggested_name"]}
             for cl in clusters_batch]

    prompt = (
        "Analisis setiap cluster berikut. Respond ONLY dengan JSON array:\n"
        "[{\"cluster_id\":\"...\",\"is_same_item\":true,\"canonical_name\":\"...\","
        "\"confidence\":\"high|medium|low\",\"reasoning\":\"...\",\"warnings\":[]}]\n\n"
        + json.dumps(items, ensure_ascii=False, indent=2)
    )

    payload = json.dumps({
        "model": api_config.get("model", "claude-sonnet-4-6"),
        "max_tokens": 2000, "temperature": 0,
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
    }).encode("utf-8")

    req = urllib.request.Request(
        f"{api_config['base_url'].rstrip('/')}/chat/completions",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_config['api_key']}",
        }, method="POST")

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = json.loads(resp.read())["choices"][0]["message"]["content"].strip()
            if "```" in raw:
                raw = re.sub(r"```(?:json)?", "", raw).strip().strip("`").strip()
            results = json.loads(raw)
            if isinstance(results, dict):
                results = [results]
            return results
    except Exception as e:
        return [{"cluster_id": cl["cluster_id"], "is_same_item": True,
                 "canonical_name": cl["suggested_name"],
                 "confidence": "low", "reasoning": f"API error: {str(e)[:60]}",
                 "warnings": []}
                for cl in clusters_batch]


def run_llm_layer(pending: list, cache: MaterialCache,
                  api_config: dict, progress_cb: Callable = None) -> dict:
    results = {}
    batches = [pending[i:i+BATCH_SIZE] for i in range(0, len(pending), BATCH_SIZE)]
    for bi, batch in enumerate(batches):
        if progress_cb:
            progress_cb(0.70 + 0.28*(bi/len(batches)),
                        f"Layer 4: Batch {bi+1}/{len(batches)}")
        batch_results = validate_batch(batch, api_config)
        rmap = {r["cluster_id"]: r for r in batch_results}
        for cl in batch:
            cid = cl["cluster_id"]
            r = rmap.get(cid, {"cluster_id": cid, "is_same_item": True,
                                "canonical_name": cl["suggested_name"],
                                "confidence": "low", "reasoning": "", "warnings": []})
            results[cid] = r
            canonical = r.get("canonical_name") or cl["suggested_name"]
            for nk in cl["norm_keys"]:
                cache.set(nk, cl["suggested_name"], canonical, "LLM", cid,
                          r.get("confidence", "low"), r.get("reasoning", ""))
    return results


class CleansingPipeline:
    def __init__(self):
        self.cache = MaterialCache()
        self._last_stats = {}
        self._last_clusters = []

    def run(self, raw_rows: list, desc_col: str, api_config: dict,
            progress_cb: Callable = None, skip_llm: bool = False) -> dict:
        if progress_cb: progress_cb(0.02, "Layer 1: Normalisasi...")

        for r in raw_rows:
            r["_normalized"] = normalize(str(r.get(desc_col, "") or ""))

        all_descs  = [str(r.get(desc_col, "") or "") for r in raw_rows]
        unique_descs = list(dict.fromkeys(all_descs))
        if progress_cb: progress_cb(0.04, f"Layer 1: {len(unique_descs)} unik dari {len(all_descs)}")

        clusters, auto_merged, cache_hits, pending_ai, layer_stats = \
            build_clusters(unique_descs, self.cache, progress_cb)

        llm_results = {}
        if pending_ai and not skip_llm and api_config.get("api_key"):
            llm_results = run_llm_layer(pending_ai, self.cache, api_config, progress_cb)
        elif pending_ai:
            for cl in pending_ai:
                llm_results[cl["cluster_id"]] = {
                    "is_same_item": True, "canonical_name": cl["suggested_name"],
                    "confidence": "low", "reasoning": "LLM dilewati",
                    "warnings": [],
                }

        result_map = {}
        for nk, ch in cache_hits.items(): result_map[nk] = ch
        for nk, am in auto_merged.items():
            if nk not in result_map: result_map[nk] = am

        for cl in clusters:
            cid = cl["cluster_id"]
            if cid in llm_results:
                r = llm_results[cid]
                canonical  = r.get("canonical_name") or cl["suggested_name"]
                confidence = r.get("confidence", "low")
                reasoning  = r.get("reasoning", "")
                for nk in cl["norm_keys"]:
                    if nk not in result_map:
                        result_map[nk] = {
                            "cleaned_text": canonical, "method": "LLM",
                            "cluster_id": cid, "confidence": confidence,
                            "reasoning": reasoning,
                            "is_same_item": r.get("is_same_item", True),
                            "warnings": r.get("warnings", []),
                        }

        for r in raw_rows:
            nk = r["_normalized"]
            if nk not in result_map:
                result_map[nk] = {
                    "cleaned_text": str(r.get(desc_col, "") or ""),
                    "method": "FALLBACK", "cluster_id": None, "confidence": "low",
                }

        if progress_cb: progress_cb(1.0, "Pipeline selesai.")

        self._last_stats = {
            **layer_stats,
            "llm_batches":  max(1, len(pending_ai)//BATCH_SIZE) if pending_ai else 0,
            "llm_clusters": len(pending_ai),
        }
        self._last_clusters = clusters
        return result_map

    def last_stats(self): return self._last_stats
    def last_clusters(self): return self._last_clusters

DEFAULT_DB_PATH = ""  # unused, kept for compat with cleaner.py import
