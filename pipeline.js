'use strict'
/**
 * pipeline.js — 4-Layer Cleansing Engine (pure JS)
 * Layer 1: Normalisasi teks
 * Layer 2: PostgreSQL cache
 * Layer 3: Fuzzy matching tiered (fuzzball)
 * Layer 4: LLM batch validation (Dinoiki/Claude)
 */

const fuzz      = require('fuzzball')
const { Pool }  = require('pg')

// ── Config ──────────────────────────────────────────────────────── //
const AUTO_MERGE_THRESHOLD = 95
const CLUSTER_THRESHOLD_LO = 75
const BATCH_SIZE           = 10

// ── Layer 1: Normalisasi ─────────────────────────────────────────── //
const NORM_RULES = [
  [/\bINCHES\b/gi, 'IN'], [/\bINCH\b/gi, 'IN'],
  [/(\d)\s*"/g, '$1IN'],  [/(\d)\s*'/g, '$1IN'],
  [/\bMILIMETER(S)?\b/gi, 'MM'], [/\bMILI\b/gi, 'MM'],
  [/\bCENTIMETER(S)?\b/gi, 'CM'], [/\bMETER(S)?\b/gi, 'M'],
  [/\bPIECES\b/gi, 'PCS'], [/\bPIECE\b/gi, 'PCS'], [/\bPC\b/gi, 'PCS'],
  [/\bUNITS\b/gi, 'UNIT'], [/\bPOUND(S)?\b/gi, 'LBS'], [/\bLB\b/gi, 'LBS'],
  [/\bSCH\s*[-.]?\s*(\d+)\b/gi, 'SCH$1'],
  [/\bCARBON\s*STEEL\b/gi, 'CS'], [/\bKARBON\b/gi, 'CARBON'],
  [/\bBAJA\b/gi, 'STEEL'], [/\bBOLA\b/gi, 'BALL'],
  [/\bKATUP\b/gi, 'VALVE'], [/\bPOMPA\b/gi, 'PUMP'],
  [/\bKOPLING\b/gi, 'COUPLING'], [/\bBANTALAN\b/gi, 'BEARING'],
  [/\bSARINGAN\b/gi, 'FILTER'], [/\bMINYAK\b/gi, 'OIL'],
  [/\bPELUMAS\b/gi, 'LUBRICANT'], [/\bOLI\b/gi, 'OIL'],
  [/\bPIPA\b/gi, 'PIPE'], [/\bKABEL\b/gi, 'CABLE'],
  [/\bINSULASI\b/gi, 'INSULATION'],
  [/\bMANOMETER\b/gi, 'PRESSURE GAUGE'],
  [/\bALAT UKUR TEKANAN\b/gi, 'PRESSURE GAUGE'],
  [/\bPERAPAT\b/gi, 'SEAL'], [/\bPACKING\b/gi, 'GASKET'],
  [/[_\-/\\]+/g, ' '], [/\s{2,}/g, ' '],
]

function normalize(text) {
  let t = String(text || '').toUpperCase().trim()
  t = t.replace(/^[^A-Z0-9]+|[^A-Z0-9]+$/g, '')
  for (const [pat, rep] of NORM_RULES) t = t.replace(pat, rep)
  return t.trim()
}

// Rule-based: BOLT M16X50 vs BOLT M16-50
function ruleBasedSame(a, b) {
  const strip = s => s.replace(/[\s\-_xX×]/g, '')
  return strip(a) === strip(b)
}

// ── Layer 2: PostgreSQL Cache ────────────────────────────────────── //
class MaterialCache {
  constructor() {
    this.pool = process.env.DATABASE_URL
      ? new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } })
      : null
    this._mem = new Map()
    if (this.pool) this._init().catch(e => console.error('[Cache init]', e.message))
    else console.log('[Cache] No DATABASE_URL, using in-memory cache')
  }

  async _init() {
    const c = await this.pool.connect()
    await c.query(`
      CREATE TABLE IF NOT EXISTS material_cache (
        normalized_key TEXT PRIMARY KEY,
        original_text  TEXT,
        cleaned_text   TEXT NOT NULL,
        method         TEXT NOT NULL,
        cluster_id     TEXT,
        confidence     TEXT,
        reasoning      TEXT,
        created_at     TIMESTAMPTZ DEFAULT NOW(),
        hit_count      INTEGER DEFAULT 1
      )`)
    await c.query(`CREATE INDEX IF NOT EXISTS idx_mc_key ON material_cache(normalized_key)`)
    c.release()
    console.log('[Cache] PostgreSQL ready')
  }

  async get(key) {
    if (this.pool) {
      try {
        const { rows } = await this.pool.query(
          'SELECT cleaned_text,method,cluster_id,confidence,reasoning FROM material_cache WHERE normalized_key=$1',
          [key])
        if (rows[0]) {
          this.pool.query('UPDATE material_cache SET hit_count=hit_count+1 WHERE normalized_key=$1', [key])
          return rows[0]
        }
      } catch(e) { console.error('[Cache.get]', e.message) }
    }
    return this._mem.get(key) || null
  }

  async bulkGet(keys) {
    if (!keys.length) return {}
    const result = {}
    if (this.pool) {
      try {
        const { rows } = await this.pool.query(
          'SELECT normalized_key,cleaned_text,method,cluster_id,confidence,reasoning FROM material_cache WHERE normalized_key=ANY($1)',
          [keys])
        for (const r of rows) result[r.normalized_key] = r
        return result
      } catch(e) { console.error('[Cache.bulkGet]', e.message) }
    }
    for (const k of keys) { const v = this._mem.get(k); if (v) result[k] = v }
    return result
  }

  async set(key, original, cleaned, method, clusterId = null, confidence = null, reasoning = null) {
    const entry = { cleaned_text: cleaned, method, cluster_id: clusterId, confidence, reasoning }
    this._mem.set(key, entry)
    if (this.pool) {
      try {
        await this.pool.query(`
          INSERT INTO material_cache(normalized_key,original_text,cleaned_text,method,cluster_id,confidence,reasoning)
          VALUES($1,$2,$3,$4,$5,$6,$7)
          ON CONFLICT(normalized_key) DO UPDATE SET
            cleaned_text=EXCLUDED.cleaned_text, method=EXCLUDED.method,
            cluster_id=EXCLUDED.cluster_id, confidence=EXCLUDED.confidence,
            reasoning=EXCLUDED.reasoning, hit_count=material_cache.hit_count+1`,
          [key, original, cleaned, method, clusterId, confidence, reasoning])
      } catch(e) { console.error('[Cache.set]', e.message) }
    }
  }

  async stats() {
    if (this.pool) {
      try {
        const { rows } = await this.pool.query('SELECT COUNT(*) as total FROM material_cache')
        const { rows: bm } = await this.pool.query('SELECT method, COUNT(*) as cnt FROM material_cache GROUP BY method')
        return { total: parseInt(rows[0].total), by_method: Object.fromEntries(bm.map(r => [r.method, parseInt(r.cnt)])) }
      } catch(e) {}
    }
    return { total: this._mem.size, by_method: {} }
  }
}

// ── Layer 3: Fuzzy Clustering ────────────────────────────────────── //
async function buildClusters(uniqueDescs, cache, progressCb) {
  const total    = uniqueDescs.length
  const normMap  = {}
  for (const d of uniqueDescs) normMap[d] = normalize(d)
  const uniqueNorms = [...new Set(Object.values(normMap))]

  // Layer 2: bulk cache lookup
  if (progressCb) progressCb(0.06, `Layer 2: cek cache ${total} item...`)
  const cached    = await cache.bulkGet(uniqueNorms)
  const cacheHits = {}
  for (const d of uniqueDescs) {
    const nk = normMap[d]
    if (cached[nk]) cacheHits[nk] = cached[nk]
  }
  if (progressCb) progressCb(0.10, `Layer 2: ${Object.keys(cacheHits).length} hit cache dari ${total}`)

  const pending      = uniqueDescs.filter(d => !cacheHits[normMap[d]])
  const pendingNorms = [...new Set(pending.map(d => normMap[d]))]

  // Layer 3: fuzzy
  const autoMerged = {}
  const clusters   = []
  const visited    = new Set()

  for (let i = 0; i < pendingNorms.length; i++) {
    const nk = pendingNorms[i]
    if (visited.has(nk)) continue
    if (progressCb && i % 10 === 0)
      progressCb(0.12 + 0.50 * (i / Math.max(pendingNorms.length, 1)),
                 `Layer 3: fuzzy ${i+1}/${pendingNorms.length}`)

    // Score semua norm keys vs nk
    const groupNks = []
    const scores   = {}
    for (const other of pendingNorms) {
      const score = fuzz.token_sort_ratio(nk, other)
      if (score >= CLUSTER_THRESHOLD_LO || ruleBasedSame(nk, other)) {
        groupNks.push(other)
        scores[other] = score
      }
    }
    for (const gnk of groupNks) visited.add(gnk)

    const origDescs = pending.filter(d => groupNks.includes(normMap[d]))
    if (!origDescs.length) continue

    // Suggested name: paling sering muncul
    const freq = {}
    for (const d of origDescs) freq[d] = (freq[d] || 0) + 1
    const suggested  = Object.entries(freq).sort((a,b) => b[1]-a[1])[0][0]
    const minScore   = Math.min(...groupNks.map(gnk => scores[gnk] ?? 100))
    const isAuto     = minScore >= AUTO_MERGE_THRESHOLD
    const isDup      = new Set(origDescs).size > 1
    const cid        = 'C' + Math.abs(hashStr(nk) % 99999).toString().padStart(5,'0')

    if (isAuto) {
      for (const gnk of groupNks) {
        autoMerged[gnk] = { cleaned_text: suggested, method: 'FUZZY_AUTO', cluster_id: cid, confidence: 'high' }
        await cache.set(gnk, suggested, suggested, 'FUZZY_AUTO', cid, 'high')
      }
    }

    clusters.push({ cluster_id: cid, norm_keys: groupNks, orig_descs: origDescs,
                    suggested_name: suggested, min_score: minScore,
                    is_duplicate: isDup, is_auto: isAuto })
  }

  const pendingAi = clusters.filter(c =>
    !c.is_auto && c.is_duplicate &&
    c.min_score >= CLUSTER_THRESHOLD_LO && c.min_score < AUTO_MERGE_THRESHOLD)

  if (progressCb) progressCb(0.65, `Layer 3 selesai. ${pendingAi.length} cluster untuk AI.`)

  return { clusters, autoMerged, cacheHits, pendingAi,
           stats: { cache_hits: Object.keys(cacheHits).length,
                    auto_merged: Object.keys(autoMerged).length,
                    total_unique: total } }
}

function hashStr(s) {
  let h = 0
  for (let i = 0; i < s.length; i++) h = Math.imul(31, h) + s.charCodeAt(i) | 0
  return h
}

// ── Layer 4: LLM Batch ───────────────────────────────────────────── //
const SYSTEM_PROMPT = `Kamu adalah expert material engineer di industri kilang minyak.
Analisis apakah deskripsi material berikut merujuk ke barang yang SAMA atau BERBEDA.
- Perbedaan bahasa/kapitalisasi/singkatan BUKAN berarti beda barang
- Perbedaan spesifikasi teknis (ukuran, rating, grade) BERARTI beda barang
Respond ONLY dengan JSON array valid, tidak ada teks lain.`

async function validateBatch(batch, apiConfig) {
  const { default: fetch } = await import('node-fetch')
  const items = batch.map(cl => ({
    cluster_id: cl.cluster_id,
    variants: cl.orig_descs.slice(0, 6),
    suggested_name: cl.suggested_name
  }))
  const prompt = `Analisis setiap cluster. Respond ONLY JSON array:
[{"cluster_id":"...","is_same_item":true,"canonical_name":"...","confidence":"high|medium|low","reasoning":"...","warnings":[]}]

${JSON.stringify(items, null, 2)}`

  try {
    const res = await fetch(`${apiConfig.base_url}/chat/completions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${apiConfig.api_key}` },
      body: JSON.stringify({
        model: apiConfig.model || 'claude-sonnet-4-6',
        max_tokens: 2000, temperature: 0,
        messages: [
          { role: 'system', content: SYSTEM_PROMPT },
          { role: 'user',   content: prompt }
        ]
      }),
      signal: AbortSignal.timeout(60000)
    })
    const data = await res.json()
    let raw = data.choices?.[0]?.message?.content?.trim() || '[]'
    raw = raw.replace(/```json?/g, '').replace(/```/g, '').trim()
    const results = JSON.parse(raw)
    return Array.isArray(results) ? results : [results]
  } catch(e) {
    return batch.map(cl => ({
      cluster_id: cl.cluster_id, is_same_item: true,
      canonical_name: cl.suggested_name,
      confidence: 'low', reasoning: `API error: ${e.message.slice(0,60)}`, warnings: []
    }))
  }
}

async function runLlmLayer(pendingAi, cache, apiConfig, progressCb) {
  const results  = {}
  const batches  = []
  for (let i = 0; i < pendingAi.length; i += BATCH_SIZE)
    batches.push(pendingAi.slice(i, i + BATCH_SIZE))

  for (let bi = 0; bi < batches.length; bi++) {
    const batch = batches[bi]
    if (progressCb) progressCb(0.70 + 0.28 * (bi / batches.length),
                               `Layer 4: Batch ${bi+1}/${batches.length}`)
    const batchResults = await validateBatch(batch, apiConfig)
    const rmap = Object.fromEntries(batchResults.map(r => [r.cluster_id, r]))
    for (const cl of batch) {
      const r = rmap[cl.cluster_id] || {
        cluster_id: cl.cluster_id, is_same_item: true,
        canonical_name: cl.suggested_name, confidence: 'low', reasoning: '', warnings: []
      }
      results[cl.cluster_id] = r
      const canonical = r.canonical_name || cl.suggested_name
      for (const nk of cl.norm_keys)
        await cache.set(nk, cl.suggested_name, canonical, 'LLM',
                        cl.cluster_id, r.confidence, r.reasoning)
    }
  }
  return results
}

// ── Main Pipeline ────────────────────────────────────────────────── //
class CleansingPipeline {
  constructor() {
    this.cache      = new MaterialCache()
    this._lastStats = {}
    this._lastClusters = []
  }

  async run(rawRows, descCol, apiConfig, progressCb, skipLlm = false) {
    if (progressCb) progressCb(0.02, 'Layer 1: Normalisasi...')

    for (const r of rawRows)
      r._normalized = normalize(String(r[descCol] || ''))

    const allDescs    = rawRows.map(r => String(r[descCol] || ''))
    const uniqueDescs = [...new Set(allDescs)]
    if (progressCb) progressCb(0.05, `Layer 1: ${uniqueDescs.length} unik dari ${allDescs.length}`)

    const { clusters, autoMerged, cacheHits, pendingAi, stats } =
      await buildClusters(uniqueDescs, this.cache, progressCb)

    let llmResults = {}
    if (pendingAi.length && !skipLlm && apiConfig?.api_key) {
      llmResults = await runLlmLayer(pendingAi, this.cache, apiConfig, progressCb)
    } else if (pendingAi.length) {
      for (const cl of pendingAi)
        llmResults[cl.cluster_id] = {
          is_same_item: true, canonical_name: cl.suggested_name,
          confidence: 'low', reasoning: 'LLM dilewati', warnings: []
        }
    }

    // Build result map
    const resultMap = {}
    for (const [nk, ch] of Object.entries(cacheHits))  resultMap[nk] = ch
    for (const [nk, am] of Object.entries(autoMerged))
      if (!resultMap[nk]) resultMap[nk] = am

    for (const cl of clusters) {
      if (!llmResults[cl.cluster_id]) continue
      const r        = llmResults[cl.cluster_id]
      const canonical = r.canonical_name || cl.suggested_name
      for (const nk of cl.norm_keys)
        if (!resultMap[nk])
          resultMap[nk] = { cleaned_text: canonical, method: 'LLM',
                            cluster_id: cl.cluster_id, confidence: r.confidence,
                            reasoning: r.reasoning, is_same_item: r.is_same_item,
                            warnings: r.warnings || [] }
    }

    for (const r of rawRows) {
      const nk = r._normalized
      if (!resultMap[nk])
        resultMap[nk] = { cleaned_text: String(r[descCol] || ''),
                          method: 'FALLBACK', cluster_id: null, confidence: 'low' }
    }

    if (progressCb) progressCb(1.0, 'Pipeline selesai.')
    this._lastStats    = { ...stats, llm_clusters: pendingAi.length,
                           llm_batches: Math.ceil(pendingAi.length / BATCH_SIZE) || 0 }
    this._lastClusters = clusters
    return resultMap
  }

  lastStats()    { return this._lastStats }
  lastClusters() { return this._lastClusters }
}

module.exports = { CleansingPipeline, normalize, MaterialCache }
