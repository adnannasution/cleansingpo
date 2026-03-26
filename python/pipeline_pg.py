"""
pipeline_pg.py — Pipeline 4-Layer dengan PostgreSQL cache.
Fixes:
  1. sslmode='require' diganti sslmode='prefer' — Railway internal tidak selalu butuh SSL
  2. hash() untuk cluster_id diganti hashlib.md5 — deterministic antar process/restart
  3. Connection pool sederhana — buka/tutup per operasi agar tidak leak
  4. progress_cb selalu dipanggil dengan 2 argumen (pct_float, msg)
"""

import os, re, json, threading, hashlib
from datetime import datetime
from typing import Callable, Optional
from collections import defaultdict, Counter
from rapidfuzz import fuzz, process as rf_process

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    HAS_PG = True
except ImportError:
    HAS_PG = False

DATABASE_URL = os.environ.get("DATABASE_URL", "")


# ════════════════════════════════════════════════════════════════════
# Layer 1 — Normalisasi
# ════════════════════════════════════════════════════════════════════

_NORM_RULES = [
    (r'\bINCHES\b','IN'),(r'\bINCH\b','IN'),(r'(?<=\d)\s*"','IN'),
    (r"(?<=\d)\s*'",'IN'),(r'\bMILIMETER(S)?\b','MM'),(r'\bMILI\b','MM'),
    (r'\bCENTIMETER(S)?\b','CM'),(r'\bMETER(S)?\b','M'),
    (r'\bPIECES\b','PCS'),(r'\bPIECE\b','PCS'),(r'\bPC\b','PCS'),
    (r'\bUNITS\b','UNIT'),(r'\bPOUND(S)?\b','LBS'),(r'\bLB\b','LBS'),
    (r'\bSCH\s*[-\.]?\s*(\d+)\b',r'SCH\1'),
    (r'\bCARBON\s*STEEL\b','CS'),(r'\bKARBON\b','CARBON'),
    (r'\bBAJA\b','STEEL'),(r'\bBOLA\b','BALL'),(r'\bKATUP\b','VALVE'),
    (r'\bPOMPA\b','PUMP'),(r'\bKOPLING\b','COUPLING'),(r'\bBANTALAN\b','BEARING'),
    (r'\bSARINGAN\b','FILTER'),(r'\bMINYAK\b','OIL'),(r'\bPELUMAS\b','LUBRICANT'),
    (r'\bOLI\b','OIL'),(r'\bPIPA\b','PIPE'),(r'\bKABEL\b','CABLE'),
    (r'\bINSULASI\b','INSULATION'),(r'\bMANOMETER\b','PRESSURE GAUGE'),
    (r'\bALAT UKUR TEKANAN\b','PRESSURE GAUGE'),(r'\bPERAPAT\b','SEAL'),
    (r'\bPACKING\b','GASKET'),
    (r'[_\-\/\\\+]+', ' '),(r'\s{2,}', ' '),
]
_COMPILED = [(re.compile(p, re.IGNORECASE), r) for p, r in _NORM_RULES]

def normalize(text: str) -> str:
    text = str(text).upper().strip()
    text = re.sub(r'^[^A-Z0-9]+|[^A-Z0-9]+$', '', text)
    for pat, repl in _COMPILED:
        text = pat.sub(repl, text)
    return text.strip()

def _stable_id(text: str) -> str:
    """Deterministic cluster ID — tidak berubah antar restart/process."""
    return "C" + hashlib.md5(text.encode()).hexdigest()[:8].upper()


# ════════════════════════════════════════════════════════════════════
# Layer 2 — PostgreSQL Cache
# ════════════════════════════════════════════════════════════════════

_CACHE_LOCK = threading.Lock()

class MaterialCache:
    def __init__(self):
        self._local = {}
        if HAS_PG and DATABASE_URL:
            self._init_pg()
        else:
            print("[Cache] PostgreSQL tidak tersedia, pakai in-memory cache.")

    def _connect(self):
        url = DATABASE_URL
        # Railway kadang pakai postgres:// — psycopg2 butuh postgresql://
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql://", 1)
        # Gunakan 'prefer' bukan 'require' — Railway internal tidak selalu SSL
        return psycopg2.connect(url, sslmode="prefer",
                                connect_timeout=10)

    def _init_pg(self):
        try:
            con = self._connect()
            with con.cursor() as cur:
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
            con.commit(); con.close()
            print("[Cache] PostgreSQL terhubung & tabel siap.")
        except Exception as e:
            print(f"[Cache] PostgreSQL init error: {e}")

    def get(self, key: str) -> Optional[dict]:
        if HAS_PG and DATABASE_URL:
            try:
                with _CACHE_LOCK:
                    con = self._connect()
                    with con.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute(
                            "SELECT cleaned_text,method,cluster_id,confidence,reasoning "
                            "FROM material_cache WHERE normalized_key=%s", (key,))
                        row = cur.fetchone()
                        if row:
                            cur.execute(
                                "UPDATE material_cache SET hit_count=hit_count+1 "
                                "WHERE normalized_key=%s", (key,))
                    con.commit(); con.close()
                if row:
                    return dict(row)
            except Exception as e:
                print(f"[Cache.get] {e}")
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
                    with con.cursor() as cur:
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
                        """, (key, original, cleaned, method,
                              cluster_id, confidence, reasoning))
                    con.commit(); con.close()
            except Exception as e:
                print(f"[Cache.set] {e}")

    def bulk_get(self, keys: list) -> dict:
        if not keys:
            return {}
        if HAS_PG and DATABASE_URL:
            try:
                with _CACHE_LOCK:
                    con = self._connect()
                    with con.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute(
                            "SELECT normalized_key,cleaned_text,method,"
                            "cluster_id,confidence,reasoning "
                            "FROM material_cache WHERE normalized_key=ANY(%s)",
                            (keys,))
                        result = {r["normalized_key"]: dict(r)
                                  for r in cur.fetchall()}
                    con.close()
                return result
            except Exception as e:
                print(f"[Cache.bulk_get] {e}")
        return {k: self._local[k] for k in keys if k in self._local}

    def stats(self) -> dict:
        if HAS_PG and DATABASE_URL:
            try:
                with _CACHE_LOCK:
                    con = self._connect()
                    with con.cursor() as cur:
                        cur.execute("SELECT COUNT(*) FROM material_cache")
                        total = cur.fetchone()[0]
                        cur.execute(
                            "SELECT method, COUNT(*) FROM material_cache GROUP BY method")
                        by_method = dict(cur.fetchall())
                    con.close()
                return {"total": total, "by_method": by_method}
            except Exception as e:
                print(f"[Cache.stats] {e}")
        return {"total": len(self._local), "by_method": {}}


# ════════════════════════════════════════════════════════════════════
# Layer 3 — Fuzzy Clustering
# ════════════════════════════════════════════════════════════════════

AUTO_MERGE_THRESHOLD = 95
CLUSTER_THRESHOLD_LO = 75
BATCH_SIZE           = 10

_SYSTEM_PROMPT = """Kamu adalah expert material engineer di industri kilang minyak (refinery).
Analisis apakah deskripsi material berikut merujuk ke barang yang SAMA atau BERBEDA.
- Perbedaan bahasa/kapitalisasi/singkatan BUKAN berarti beda barang
- Perbedaan spesifikasi teknis (ukuran, rating, schedule, grade) BERARTI beda barang
Respond ONLY dengan JSON array valid, tanpa markdown."""


def _rule_based(a: str, b: str) -> bool:
    def strip(s): return re.sub(r'[\s\-_xX×]', '', s)
    return strip(a) == strip(b)


def build_clusters(unique_descs: list, cache: MaterialCache,
                   progress_cb: Callable = None) -> tuple:
    total    = len(unique_descs)
    norm_map = {d: normalize(d) for d in unique_descs}
    all_norms = list(dict.fromkeys(norm_map.values()))

    # Layer 2: bulk cache lookup
    if progress_cb:
        progress_cb(0.08, f"Layer 2: cache lookup {total} item...")
    cached     = cache.bulk_get(all_norms)
    cache_hits = {nk: cached[nk] for nk in all_norms if nk in cached}

    if progress_cb:
        progress_cb(0.12, f"Layer 2: {len(cache_hits)}/{total} hit cache ✓")

    pending_norms = [nk for nk in all_norms if nk not in cache_hits]

    # Layer 3: fuzzy clustering pada sisa
    auto_merged = {}
    clusters    = []
    visited     = set()
    n_pending   = len(pending_norms)

    for i, nk in enumerate(pending_norms):
        if nk in visited:
            continue
        if progress_cb and (i % max(1, n_pending // 20) == 0):
            pct = 0.12 + 0.50 * (i / max(n_pending, 1))
            progress_cb(pct, f"Layer 3: fuzzy {i+1}/{n_pending}...")

        matches    = rf_process.extract(
            nk, pending_norms, scorer=fuzz.token_sort_ratio,
            score_cutoff=CLUSTER_THRESHOLD_LO)
        group_nks  = [m[0] for m in matches]
        group_scores = {m[0]: m[1] for m in matches}
        visited.update(group_nks)

        # Kumpulkan orig_descs untuk group ini
        orig_descs = [d for d in unique_descs
                      if norm_map[d] in group_nks]
        if not orig_descs:
            continue

        min_score  = min(group_scores.values()) if group_scores else 100
        freq       = Counter(orig_descs)
        suggested  = freq.most_common(1)[0][0]
        is_auto    = (min_score >= AUTO_MERGE_THRESHOLD
                      or len(group_nks) == 1
                      or _rule_based(group_nks[0],
                                     group_nks[1] if len(group_nks) > 1 else group_nks[0]))
        is_dup     = len(set(orig_descs)) > 1

        # Gunakan MD5-based ID — deterministic
        cid = _stable_id(nk)

        if is_auto:
            for gnk in group_nks:
                method = "FUZZY_AUTO" if is_dup else "FUZZY_SINGLE"
                auto_merged[gnk] = {
                    "cleaned_text": suggested, "method": method,
                    "cluster_id": cid, "confidence": "high",
                }
                cache.set(gnk, suggested, suggested, method, cid, "high")

        clusters.append({
            "cluster_id":   cid,
            "norm_keys":    group_nks,
            "orig_descs":   orig_descs,
            "suggested_name": suggested,
            "min_score":    min_score,
            "is_duplicate": is_dup,
            "is_auto":      is_auto,
        })

    pending_ai = [
        c for c in clusters
        if not c["is_auto"] and c["is_duplicate"]
        and CLUSTER_THRESHOLD_LO <= c["min_score"] < AUTO_MERGE_THRESHOLD
    ]

    if progress_cb:
        progress_cb(0.65, f"Layer 3 selesai ✓  {len(auto_merged)} auto-merge, "
                           f"{len(pending_ai)} untuk AI")

    return clusters, auto_merged, cache_hits, pending_ai, {
        "cache_hits":  len(cache_hits),
        "auto_merged": len(auto_merged),
        "total_unique": total,
    }


# ════════════════════════════════════════════════════════════════════
# Layer 4 — LLM Batch
# ════════════════════════════════════════════════════════════════════

def validate_batch(clusters_batch: list, api_config: dict) -> list:
    import urllib.request
    items = [
        {"cluster_id":    cl["cluster_id"],
         "variants":      cl["orig_descs"][:6],
         "suggested_name": cl["suggested_name"]}
        for cl in clusters_batch
    ]
    prompt = (
        "Analisis setiap cluster berikut. Respond ONLY dengan JSON array:\n"
        '[{"cluster_id":"...","is_same_item":true,"canonical_name":"...",'
        '"confidence":"high|medium|low","reasoning":"...","warnings":[]}]\n\n'
        + json.dumps(items, ensure_ascii=False, indent=2)
    )
    payload = json.dumps({
        "model":      api_config.get("model", "claude-sonnet-4-6"),
        "max_tokens": 2000,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user",   "content": prompt},
        ],
    }).encode("utf-8")

    req = urllib.request.Request(
        f"{api_config['base_url'].rstrip('/')}/chat/completions",
        data=payload,
        headers={
            "Content-Type":  "application/json",
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
        print(f"[LLM] batch error: {e}")
        return [
            {"cluster_id":    cl["cluster_id"],
             "is_same_item":  True,
             "canonical_name": cl["suggested_name"],
             "confidence":    "low",
             "reasoning":     f"API error: {str(e)[:80]}",
             "warnings":      []}
            for cl in clusters_batch
        ]


def run_llm_layer(pending: list, cache: MaterialCache,
                  api_config: dict, progress_cb: Callable = None) -> dict:
    results = {}
    n       = len(pending)
    batches = [pending[i:i+BATCH_SIZE] for i in range(0, n, BATCH_SIZE)]
    nb      = len(batches)

    for bi, batch in enumerate(batches):
        if progress_cb:
            progress_cb(
                0.70 + 0.28 * (bi / nb),
                f"Layer 4: AI batch {bi+1}/{nb} ({len(batch)} cluster)...")

        batch_res = validate_batch(batch, api_config)
        rmap      = {r["cluster_id"]: r for r in batch_res}

        for cl in batch:
            cid = cl["cluster_id"]
            r   = rmap.get(cid, {
                "cluster_id": cid, "is_same_item": True,
                "canonical_name": cl["suggested_name"],
                "confidence": "low", "reasoning": "", "warnings": [],
            })
            results[cid] = r
            canonical = r.get("canonical_name") or cl["suggested_name"]
            for nk in cl["norm_keys"]:
                cache.set(nk, cl["suggested_name"], canonical, "LLM",
                          cid, r.get("confidence","low"), r.get("reasoning",""))

    if progress_cb:
        progress_cb(0.98, f"Layer 4 selesai ✓  {n} cluster divalidasi AI")
    return results


# ════════════════════════════════════════════════════════════════════
# Main Pipeline Orchestrator
# ════════════════════════════════════════════════════════════════════

class CleansingPipeline:
    def __init__(self):
        self.cache          = MaterialCache()
        self._last_stats    = {}
        self._last_clusters = []

    def run(self, raw_rows: list, desc_col: str, api_config: dict,
            progress_cb: Callable = None, skip_llm: bool = False) -> dict:

        if progress_cb:
            progress_cb(0.02, "Layer 1: Normalisasi teks...")

        for r in raw_rows:
            r["_normalized"] = normalize(str(r.get(desc_col, "") or ""))

        all_descs    = [str(r.get(desc_col, "") or "") for r in raw_rows]
        unique_descs = list(dict.fromkeys(all_descs))

        if progress_cb:
            progress_cb(0.04,
                f"Layer 1: {len(all_descs)} baris → {len(unique_descs)} unik ✓")

        clusters, auto_merged, cache_hits, pending_ai, layer_stats = \
            build_clusters(unique_descs, self.cache, progress_cb)

        # Layer 4: LLM
        llm_results = {}
        if pending_ai and not skip_llm and api_config.get("api_key"):
            llm_results = run_llm_layer(
                pending_ai, self.cache, api_config, progress_cb)
        elif pending_ai:
            # Offline fallback
            for cl in pending_ai:
                llm_results[cl["cluster_id"]] = {
                    "is_same_item":  True,
                    "canonical_name": cl["suggested_name"],
                    "confidence":    "low",
                    "reasoning":     "LLM dilewati (offline)",
                    "warnings":      [],
                }

        # Build result_map
        result_map = {}
        for nk, ch in cache_hits.items():
            result_map[nk] = ch
        for nk, am in auto_merged.items():
            if nk not in result_map:
                result_map[nk] = am

        for cl in clusters:
            cid = cl["cluster_id"]
            if cid in llm_results:
                r         = llm_results[cid]
                canonical = r.get("canonical_name") or cl["suggested_name"]
                for nk in cl["norm_keys"]:
                    if nk not in result_map:
                        result_map[nk] = {
                            "cleaned_text": canonical,
                            "method":       "LLM",
                            "cluster_id":   cid,
                            "confidence":   r.get("confidence", "low"),
                            "reasoning":    r.get("reasoning", ""),
                            "is_same_item": r.get("is_same_item", True),
                            "warnings":     r.get("warnings", []),
                        }

        # Fallback untuk yang belum dapat result
        for r in raw_rows:
            nk = r["_normalized"]
            if nk not in result_map:
                result_map[nk] = {
                    "cleaned_text": str(r.get(desc_col, "") or ""),
                    "method":       "FALLBACK",
                    "cluster_id":   None,
                    "confidence":   "low",
                }

        if progress_cb:
            progress_cb(1.0, "✅ Pipeline selesai.")

        self._last_stats = {
            **layer_stats,
            "llm_batches":  max(1, len(pending_ai)//BATCH_SIZE) if pending_ai else 0,
            "llm_clusters": len(pending_ai),
        }
        self._last_clusters = clusters
        return result_map

    def last_stats(self)    -> dict: return self._last_stats
    def last_clusters(self) -> list: return self._last_clusters


DEFAULT_DB_PATH = ""  # kept for compat with cleaner.py import