"""
Processing Pipeline — 4-Layer Architecture
Layer 1: Pre-Processing & Normalization (Local)
Layer 2: Persistent Cache (SQLite)
Layer 3: Tiered Fuzzy Matching (RapidFuzz)
Layer 4: LLM Batch Validation (Multi-cluster per API call)
"""

import re
import sqlite3
import json
import os
import threading
from collections import defaultdict
from datetime import datetime
from typing import Callable, Optional

from rapidfuzz import fuzz, process as rf_process


# ══════════════════════════════════════════════════════════════════════
# LAYER 1 — NORMALIZATION
# ══════════════════════════════════════════════════════════════════════

# Mapping: pattern → canonical form (semua ke bentuk standar)
_NORM_RULES = [
    # Satuan ukuran — standarisasi ke bentuk pendek
    (r'\bINCHES\b',          'IN'),
    (r'\bINCH\b',            'IN'),
    (r'(?<=\d)\s*"',         'IN'),   # 3" → 3IN
    (r"(?<=\d)\s*'",         'IN'),
    (r'\bMILIMETER(S)?\b',   'MM'),
    (r'\bMILI\b',            'MM'),
    (r'\bCENTIMETER(S)?\b',  'CM'),
    (r'\bMETER(S)?\b',       'M'),
    # Satuan jumlah
    (r'\bPIECES\b',          'PCS'),
    (r'\bPIECE\b',           'PCS'),
    (r'\bPC\b',              'PCS'),
    (r'\bUNITS\b',           'UNIT'),
    (r'\bPOUND(S)?\b',       'LBS'),
    (r'\bLB\b',              'LBS'),
    # Schedule pipa
    (r'\bSCH\s*[-\.]?\s*(\d+)\b', r'SCH\1'),   # SCH-40 / SCH.40 → SCH40
    # Material
    (r'\bCARBON\s*STEEL\b',  'CS'),
    (r'\bKARBON\b',          'CARBON'),
    # Bahasa Indonesia → Inggris
    (r'\bBAJA\b',            'STEEL'),
    (r'\bBOLA\b',            'BALL'),
    (r'\bKATUP\b',           'VALVE'),
    (r'\bPOMPA\b',           'PUMP'),
    (r'\bKOPLING\b',         'COUPLING'),
    (r'\bBANTALAN\b',        'BEARING'),
    (r'\bSARINGAN\b',        'FILTER'),
    (r'\bMINYAK\b',          'OIL'),
    (r'\bPELUMAS\b',         'LUBRICANT'),
    (r'\bOLI\b',             'OIL'),
    (r'\bPIPA\b',            'PIPE'),
    (r'\bKABEL\b',           'CABLE'),
    (r'\bINSULASI\b',        'INSULATION'),
    (r'\bMANOMETER\b',       'PRESSURE GAUGE'),
    (r'\bALAT UKUR TEKANAN\b','PRESSURE GAUGE'),
    (r'\bPERAPAT\b',         'SEAL'),
    (r'\bPACKING\b',         'GASKET'),
    # Separator → spasi
    (r'[_\-\/\\]+',          ' '),
    # Whitespace
    (r'\s{2,}',              ' '),
]

_COMPILED_RULES = [(re.compile(p, re.IGNORECASE), r) for p, r in _NORM_RULES]


def normalize(text: str) -> str:
    """Layer 1: Normalize raw material description."""
    text = str(text).upper().strip()
    # Hapus karakter non-alfanumerik di awal/akhir
    text = re.sub(r'^[^A-Z0-9]+|[^A-Z0-9]+$', '', text)
    for pattern, repl in _COMPILED_RULES:
        text = pattern.sub(repl, text)
    return text.strip()


# ══════════════════════════════════════════════════════════════════════
# LAYER 2 — SQLITE CACHE
# ══════════════════════════════════════════════════════════════════════

_CACHE_LOCK = threading.Lock()

DEFAULT_DB_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), '..', 'material_cache.db'
)


class MaterialCache:
    """
    Persistent SQLite cache.
    Key = normalized_text (not original), sehingga variasi minor tetap hit cache.
    """

    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        self.db_path = os.path.abspath(db_path)
        self._init_db()

    def _connect(self):
        return sqlite3.connect(self.db_path, check_same_thread=False)

    def _init_db(self):
        with _CACHE_LOCK:
            con = self._connect()
            con.execute("""
                CREATE TABLE IF NOT EXISTS material_cache (
                    normalized_key  TEXT PRIMARY KEY,
                    original_text   TEXT,
                    cleaned_text    TEXT NOT NULL,
                    method          TEXT NOT NULL,
                    cluster_id      TEXT,
                    confidence      TEXT,
                    created_at      TEXT,
                    hit_count       INTEGER DEFAULT 1
                )
            """)
            con.execute("""
                CREATE INDEX IF NOT EXISTS idx_normalized
                ON material_cache(normalized_key)
            """)
            con.commit()
            con.close()

    def get(self, normalized_key: str) -> Optional[dict]:
        with _CACHE_LOCK:
            con = self._connect()
            row = con.execute(
                "SELECT cleaned_text, method, cluster_id, confidence "
                "FROM material_cache WHERE normalized_key = ?",
                (normalized_key,)
            ).fetchone()
            if row:
                con.execute(
                    "UPDATE material_cache SET hit_count = hit_count + 1 "
                    "WHERE normalized_key = ?", (normalized_key,))
                con.commit()
            con.close()
        if row:
            return {
                "cleaned_text": row[0], "method": row[1],
                "cluster_id": row[2], "confidence": row[3],
            }
        return None

    def set(self, normalized_key: str, original_text: str,
            cleaned_text: str, method: str,
            cluster_id: str = None, confidence: str = None):
        with _CACHE_LOCK:
            con = self._connect()
            con.execute("""
                INSERT OR REPLACE INTO material_cache
                (normalized_key, original_text, cleaned_text, method,
                 cluster_id, confidence, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (normalized_key, original_text, cleaned_text, method,
                  cluster_id, confidence, datetime.now().isoformat()))
            con.commit()
            con.close()

    def bulk_get(self, normalized_keys: list) -> dict:
        """Ambil banyak key sekaligus — lebih efisien dari get() satu per satu."""
        if not normalized_keys:
            return {}
        with _CACHE_LOCK:
            con = self._connect()
            placeholders = ",".join("?" * len(normalized_keys))
            rows = con.execute(
                f"SELECT normalized_key, cleaned_text, method, cluster_id, confidence "
                f"FROM material_cache WHERE normalized_key IN ({placeholders})",
                normalized_keys
            ).fetchall()
            con.close()
        return {
            r[0]: {"cleaned_text": r[1], "method": r[2],
                   "cluster_id": r[3], "confidence": r[4]}
            for r in rows
        }

    def stats(self) -> dict:
        with _CACHE_LOCK:
            con = self._connect()
            total = con.execute("SELECT COUNT(*) FROM material_cache").fetchone()[0]
            by_method = con.execute(
                "SELECT method, COUNT(*) FROM material_cache GROUP BY method"
            ).fetchall()
            con.close()
        return {"total": total, "by_method": dict(by_method)}

    def clear(self):
        with _CACHE_LOCK:
            con = self._connect()
            con.execute("DELETE FROM material_cache")
            con.commit()
            con.close()


# ══════════════════════════════════════════════════════════════════════
# LAYER 3 — TIERED FUZZY MATCHING
# ══════════════════════════════════════════════════════════════════════

AUTO_MERGE_THRESHOLD  = 95   # ≥95% → auto-merge, no AI
CLUSTER_THRESHOLD_HI  = 90   # 75-94% → cluster untuk AI
CLUSTER_THRESHOLD_LO  = 75


def _rule_based_normalize(a: str, b: str) -> bool:
    """
    Layer 3.5 — Rule-based check sebelum AI.
    Tangani pola seperti BOLT M16X50 vs BOLT M16-50.
    """
    def strip_separators(s):
        return re.sub(r'[\s\-_xX×]', '', s)
    return strip_separators(a) == strip_separators(b)


def build_clusters(unique_descs: list,
                   cache: MaterialCache,
                   progress_cb: Callable = None) -> tuple:
    """
    Layer 3: Kelompokkan deskripsi unik ke dalam cluster.

    Returns:
        clusters       : list[dict] — cluster hasil grouping
        auto_merged    : dict — normalized_key → cleaned_text (resolved locally)
        cache_hits     : dict — normalized_key → cache result
        pending_ai     : list[dict] — cluster yang perlu AI
        stats          : dict — ringkasan per-layer
    """
    total = len(unique_descs)
    norm_map = {d: normalize(d) for d in unique_descs}   # original → normalized
    norm_keys = list(norm_map.values())

    # ── Layer 2: Cache lookup (bulk) ─────────────────────────────────
    if progress_cb:
        progress_cb(0.05, "Layer 2: Cache lookup…")

    cache_results = cache.bulk_get(norm_keys)
    cache_hits = {}
    remaining = []

    for orig, norm_key in norm_map.items():
        if norm_key in cache_results:
            cache_hits[norm_key] = {**cache_results[norm_key], "original": orig}
        else:
            remaining.append((orig, norm_key))

    if progress_cb:
        progress_cb(0.15, f"Layer 2: {len(cache_hits)}/{total} dari cache ✓")

    # ── Layer 3: Fuzzy clustering pada sisa ──────────────────────────
    if progress_cb:
        progress_cb(0.20, "Layer 3: Fuzzy matching…")

    remaining_norms = [nk for _, nk in remaining]
    visited = set()
    raw_groups = []

    for i, (orig, norm_key) in enumerate(remaining):
        if progress_cb and i % max(1, len(remaining) // 20) == 0:
            pct = 0.20 + 0.50 * (i / max(1, len(remaining)))
            progress_cb(pct, f"Layer 3: Fuzzy {i+1}/{len(remaining)}…")

        if norm_key in visited:
            continue

        matches = rf_process.extract(
            norm_key, remaining_norms,
            scorer=fuzz.token_set_ratio,
            score_cutoff=CLUSTER_THRESHOLD_LO
        )
        group_keys = {m[0] for m in matches}
        group_origs = [o for o, nk in remaining if nk in group_keys]

        # Auto-merge jika semua anggota ≥ AUTO_MERGE_THRESHOLD satu sama lain
        scores_within = []
        group_norm_list = list(group_keys)
        for gi in range(len(group_norm_list)):
            for gj in range(gi + 1, len(group_norm_list)):
                s = fuzz.token_set_ratio(group_norm_list[gi], group_norm_list[gj])
                scores_within.append(s)

        min_score = min(scores_within) if scores_within else 100
        is_auto = min_score >= AUTO_MERGE_THRESHOLD or len(group_keys) == 1

        # Rule-based check untuk grup kecil
        if not is_auto and len(group_origs) == 2:
            if _rule_based_normalize(group_norm_list[0], group_norm_list[1]):
                is_auto = True

        # Pilih canonical: frekuensi terbanyak di group
        freq = defaultdict(int)
        for o in group_origs:
            freq[o] += 1
        canonical_orig = max(freq, key=freq.get)
        canonical_norm = norm_map[canonical_orig]

        raw_groups.append({
            "orig_descs": group_origs,
            "norm_keys": list(group_keys),
            "canonical_orig": canonical_orig,
            "canonical_norm": canonical_norm,
            "min_score": min_score,
            "is_auto": is_auto,
        })
        visited.update(group_keys)

    # ── Pisahkan auto-merge vs pending AI ────────────────────────────
    auto_merged = {}
    pending_ai = []
    clusters = []

    for gid, grp in enumerate(raw_groups):
        cluster_id = f"C{gid:04d}"
        if grp["is_auto"]:
            # Simpan ke cache langsung
            for nk in grp["norm_keys"]:
                method = "FUZZY_AUTO" if len(grp["norm_keys"]) > 1 else "FUZZY_SINGLE"
                auto_merged[nk] = {
                    "cleaned_text": grp["canonical_orig"],
                    "method": method,
                    "cluster_id": cluster_id,
                    "confidence": "high",
                }
                cache.set(nk, grp["canonical_orig"], grp["canonical_orig"],
                          method, cluster_id, "high")
        else:
            pending_ai.append({
                "cluster_id": cluster_id,
                "orig_descs": grp["orig_descs"],
                "norm_keys": grp["norm_keys"],
                "suggested_name": grp["canonical_orig"],
                "min_score": grp["min_score"],
            })

        clusters.append({
            "cluster_id": cluster_id,
            "orig_descs": grp["orig_descs"],
            "norm_keys": grp["norm_keys"],
            "suggested_name": grp["canonical_orig"],
            "min_score": grp["min_score"],
            "is_auto": grp["is_auto"],
            "is_duplicate": len(set(grp["orig_descs"])) > 1,
        })

    stats = {
        "total_unique": total,
        "cache_hits": len(cache_hits),
        "auto_merged": sum(1 for g in raw_groups if g["is_auto"]),
        "pending_ai": len(pending_ai),
        "total_clusters": len(clusters),
    }

    if progress_cb:
        progress_cb(0.70, f"Layer 3 selesai. {stats['auto_merged']} auto-merge, {len(pending_ai)} perlu AI…")

    return clusters, auto_merged, cache_hits, pending_ai, stats


# ══════════════════════════════════════════════════════════════════════
# LAYER 4 — LLM BATCH VALIDATION
# ══════════════════════════════════════════════════════════════════════

BATCH_SIZE = 10   # Jumlah cluster per satu API call

_SYSTEM_PROMPT = """Kamu adalah expert material engineer di industri kilang minyak (refinery).
Tugasmu menganalisis grup-grup deskripsi material dan menentukan SATU canonical name (nama standar teknis) untuk setiap grup.

Rules:
- Nama kanonik harus dalam BAHASA INGGRIS, format UPPERCASE
- Gunakan terminologi teknis standar industri
- Jika item dalam grup BERBEDA (bukan variasi nama yang sama), set is_same_item = false dan canonical_name = null
- Respond HANYA dengan JSON valid, tanpa penjelasan tambahan, tanpa markdown."""


def _build_batch_prompt(clusters_batch: list) -> str:
    lines = ["Analisis setiap grup berikut:\n"]
    for cl in clusters_batch:
        lines.append(f"Grup {cl['cluster_id']}:")
        for i, desc in enumerate(cl["orig_descs"], 1):
            lines.append(f"  {i}. {desc}")
        lines.append(f"  Nama saran sistem: {cl['suggested_name']}\n")

    lines.append("""Respond dengan JSON array PERSIS seperti ini:
[
  {
    "cluster_id": "C0001",
    "is_same_item": true,
    "canonical_name": "NAMA STANDAR",
    "confidence": "high",
    "reasoning": "singkat"
  }
]""")
    return "\n".join(lines)


def validate_batch(clusters_batch: list, api_config: dict) -> list:
    """
    Kirim satu batch (beberapa cluster) ke LLM dalam SATU API call.
    Returns list of result dicts, satu per cluster.
    """
    import requests

    prompt = _build_batch_prompt(clusters_batch)
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_config['api_key']}",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }
    payload = {
        "model": api_config.get("model", "claude-sonnet-4-6"),
        "max_tokens": 2000,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
    }

    try:
        resp = requests.post(
            f"{api_config['base_url'].rstrip('/')}/chat/completions",
            headers=headers, json=payload, timeout=60
        )
        resp.raise_for_status()
        raw_text = resp.json()["choices"][0]["message"]["content"].strip()

        # Strip markdown jika ada
        if "```" in raw_text:
            raw_text = re.sub(r"```(?:json)?", "", raw_text).strip().strip("`").strip()

        results = json.loads(raw_text)
        if isinstance(results, dict):
            results = [results]
        return results

    except Exception as e:
        # Fallback: kembalikan suggested_name untuk semua cluster di batch ini
        return [
            {
                "cluster_id": cl["cluster_id"],
                "is_same_item": True,
                "canonical_name": cl["suggested_name"],
                "confidence": "low",
                "reasoning": f"API error — fallback ke suggested name: {str(e)[:80]}",
            }
            for cl in clusters_batch
        ]


def run_llm_layer(pending_clusters: list, cache: MaterialCache,
                  api_config: dict, progress_cb: Callable = None) -> dict:
    """
    Layer 4: Proses semua pending cluster dengan LLM, BATCH_SIZE cluster per call.
    Returns dict: cluster_id → result
    """
    results = {}
    total = len(pending_clusters)
    if total == 0:
        return results

    batches = [
        pending_clusters[i:i + BATCH_SIZE]
        for i in range(0, total, BATCH_SIZE)
    ]
    n_batches = len(batches)

    for bi, batch in enumerate(batches):
        if progress_cb:
            pct = 0.70 + 0.28 * (bi / n_batches)
            names = ", ".join(c["suggested_name"][:20] for c in batch[:3])
            progress_cb(pct, f"Layer 4: Batch {bi+1}/{n_batches} → [{names}…]")

        batch_results = validate_batch(batch, api_config)

        # Index hasil berdasarkan cluster_id
        result_map = {r["cluster_id"]: r for r in batch_results}

        for cl in batch:
            cid = cl["cluster_id"]
            r = result_map.get(cid, {
                "cluster_id": cid,
                "is_same_item": True,
                "canonical_name": cl["suggested_name"],
                "confidence": "low",
                "reasoning": "Result tidak ditemukan di response AI",
            })
            results[cid] = r

            # Simpan ke cache
            canonical = r.get("canonical_name") or cl["suggested_name"]
            confidence = r.get("confidence", "low")
            for nk in cl["norm_keys"]:
                cache.set(nk, cl["suggested_name"], canonical,
                          "LLM", cid, confidence)

    if progress_cb:
        progress_cb(0.98, f"Layer 4 selesai. {total} cluster divalidasi AI.")

    return results


# ══════════════════════════════════════════════════════════════════════
# MAIN PIPELINE ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════

class CleansingPipeline:
    """
    Orkestrasi 4 layer. Dipakai oleh POCleaner sebagai engine utama.
    """

    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        self.cache = MaterialCache(db_path)

    def run(self, raw_rows: list, desc_col: str,
            api_config: dict,
            progress_cb: Callable = None,
            skip_llm: bool = False) -> dict:
        """
        Proses pipeline penuh.

        Args:
            raw_rows    : list of dicts (dari POCleaner.raw_data)
            desc_col    : nama kolom deskripsi
            api_config  : {api_key, base_url, model}
            progress_cb : callback(pct: float, msg: str)
            skip_llm    : jika True, skip Layer 4 (mode offline)

        Returns:
            result_map  : normalized_key → {cleaned_text, method, cluster_id, confidence}
        """
        if progress_cb:
            progress_cb(0.02, "Layer 1: Normalisasi teks…")

        # ── Layer 1: Normalize + deduplikasi ─────────────────────────
        all_descs = [str(r.get(desc_col, "") or "") for r in raw_rows]
        unique_descs = list(dict.fromkeys(all_descs))   # preserve order

        # Tambahkan normalisasi ke setiap baris
        for r in raw_rows:
            r["_normalized"] = normalize(str(r.get(desc_col, "") or ""))

        if progress_cb:
            progress_cb(0.04, f"Layer 1: {len(all_descs)} baris → {len(unique_descs)} unik ✓")

        # ── Layer 2 + 3: Cache + Fuzzy clustering ────────────────────
        clusters, auto_merged, cache_hits, pending_ai, layer_stats = build_clusters(
            unique_descs, self.cache, progress_cb
        )

        # ── Layer 4: LLM (jika ada pending dan tidak skip) ───────────
        llm_results = {}
        if pending_ai and not skip_llm and api_config.get("api_key"):
            llm_results = run_llm_layer(pending_ai, self.cache, api_config, progress_cb)
        elif pending_ai and (skip_llm or not api_config.get("api_key")):
            # Fallback: pakai suggested_name untuk semua pending
            for cl in pending_ai:
                llm_results[cl["cluster_id"]] = {
                    "cluster_id": cl["cluster_id"],
                    "is_same_item": True,
                    "canonical_name": cl["suggested_name"],
                    "confidence": "low",
                    "reasoning": "LLM dilewati (mode offline)",
                }

        # ── Bangun result_map ─────────────────────────────────────────
        # Prioritas: cache_hits > auto_merged > llm_results
        result_map = {}

        # Dari cache
        for nk, ch in cache_hits.items():
            result_map[nk] = ch

        # Dari auto-merge
        for nk, am in auto_merged.items():
            if nk not in result_map:
                result_map[nk] = am

        # Dari LLM — map cluster_id → semua norm_keys di cluster tersebut
        # Kita perlu reverse-lookup: norm_key → cluster_id
        nk_to_cluster = {}
        for cl in clusters:
            for nk in cl["norm_keys"]:
                nk_to_cluster[nk] = cl

        for cid, llm_r in llm_results.items():
            canonical = llm_r.get("canonical_name") or ""
            confidence = llm_r.get("confidence", "low")
            # Temukan semua norm_keys untuk cluster ini
            for cl in clusters:
                if cl["cluster_id"] == cid:
                    for nk in cl["norm_keys"]:
                        if nk not in result_map:
                            result_map[nk] = {
                                "cleaned_text": canonical or cl["suggested_name"],
                                "method": "LLM",
                                "cluster_id": cid,
                                "confidence": confidence,
                            }

        # Fallback untuk norm_key yang tidak ada di result_map sama sekali
        for r in raw_rows:
            nk = r["_normalized"]
            if nk not in result_map:
                result_map[nk] = {
                    "cleaned_text": str(r.get(desc_col, "") or ""),
                    "method": "FALLBACK",
                    "cluster_id": None,
                    "confidence": "low",
                }

        if progress_cb:
            progress_cb(1.0, "✅ Pipeline selesai.")

        # Simpan metadata untuk reporting
        self._last_stats = {
            **layer_stats,
            "llm_batches": max(1, len(pending_ai) // BATCH_SIZE) if pending_ai else 0,
            "llm_clusters": len(pending_ai),
        }
        self._last_clusters = clusters

        return result_map

    def last_stats(self) -> dict:
        return getattr(self, "_last_stats", {})

    def last_clusters(self) -> list:
        return getattr(self, "_last_clusters", [])
