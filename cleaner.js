'use strict'
const { CleansingPipeline, normalize } = require('./pipeline')

class POCleaner {
  constructor() {
    this.rawData        = []
    this.clusters       = []
    this.decisions      = {}
    this.headerRow      = []
    this.descColName    = null
    this.availableCols  = []
    this._pipeline      = new CleansingPipeline()
    this._resultMap     = {}
    this._pipelineStats = {}
  }

  loadExcel(workbook) {
    try {
      const XLSX    = require('xlsx')
      const ws      = workbook.Sheets[workbook.SheetNames[0]]
      const rows    = XLSX.utils.sheet_to_json(ws, { defval: null, raw: false })
      if (!rows.length) return { ok: false, message: 'File kosong.' }

      this.headerRow     = Object.keys(rows[0])
      this.availableCols = this.headerRow.filter(h => h && !h.startsWith('_'))
      this.rawData       = rows.map((r, i) => ({ ...r, _row_id: i }))
      this.clusters      = []; this.decisions = {}; this._resultMap = {}
      this.descColName   = null

      const kw    = ['deskripsi','description','material','nama','uraian','item','barang']
      const auto  = this.availableCols.find(h => kw.some(k => h.toLowerCase().includes(k))) || null
      return { ok: true, count: this.rawData.length,
               columns: this.availableCols, auto_detect: auto }
    } catch(e) { return { ok: false, message: e.message } }
  }

  setDescColumn(col) {
    if (!this.headerRow.includes(col))
      return { ok: false, message: `Kolom '${col}' tidak ditemukan.` }
    this.descColName = col
    for (const r of this.rawData)
      r._normalized = normalize(String(r[col] || ''))
    this.clusters = []; this.decisions = {}; this._resultMap = {}
    return { ok: true }
  }

  async runPipeline(apiConfig, threshold = 75, progressCb, skipLlm = false) {
    if (!this.descColName) return { clusters: [], dupCount: 0 }
    this._resultMap     = await this._pipeline.run(
      this.rawData, this.descColName, apiConfig, progressCb, skipLlm)
    this._pipelineStats = this._pipeline.lastStats()
    this.clusters       = this._convertClusters(this._pipeline.lastClusters())
    const dupCount      = this.clusters.filter(c => c.is_duplicate).length
    return { clusters: this.clusters, dupCount }
  }

  _convertClusters(rawClusters) {
    const descCol    = this.descColName
    const descToRows = {}
    for (const r of this.rawData) {
      const k = String(r[descCol] || '')
      if (!descToRows[k]) descToRows[k] = []
      descToRows[k].push(r)
    }

    const result = []
    for (const cl of rawClusters) {
      const members = cl.orig_descs.flatMap(d => descToRows[d] || [])
      let canonical  = cl.suggested_name
      let method     = 'FUZZY_AUTO', confidence = 'high'
      let reasoning  = '', warnings = [], isSame = true

      for (const nk of cl.norm_keys) {
        const rm = this._resultMap[nk]
        if (rm) {
          if (rm.cleaned_text) canonical = rm.cleaned_text
          method     = rm.method     || method
          confidence = rm.confidence || confidence
          reasoning  = rm.reasoning  || ''
          warnings   = rm.warnings   || []
          isSame     = rm.is_same_item !== false
          break
        }
      }

      const aiResult = ['LLM','CACHE'].includes(method) && canonical
        ? { is_same_item: isSame, confidence, canonical_name: canonical,
            reasoning, warnings }
        : null

      const id = Math.abs(hashStr(cl.cluster_id)) % 100000
      result.push({
        id, cluster_id: cl.cluster_id,
        norm_keys: cl.norm_keys,
        orig_descs: [...new Set(cl.orig_descs)],
        suggested_name: canonical,
        avg_score: Math.round(cl.min_score * 10) / 10,
        is_duplicate: cl.is_duplicate,
        is_auto: cl.is_auto || false,
        method, confidence, ai_result: aiResult,
        items: members,
        items_count: members.length,
        ru_list: [...new Set(members.map(r => String(r['Sumber RU'] || '')).filter(Boolean))].sort()
      })
    }
    result.sort((a, b) => (b.is_duplicate - a.is_duplicate) || (a.avg_score - b.avg_score))
    return result
  }

  saveDecision(clusterId, action, canonicalName) {
    this.decisions[clusterId] = { action, canonical_name: canonicalName,
                                   timestamp: new Date().toISOString() }
  }

  getDecision(clusterId) { return this.decisions[clusterId] || null }

  applyDecisions() {
    const descCol  = this.descColName || 'Deskripsi Material'
    const cleanCol = `${descCol} (Clean)`
    const rowFinal = {}
    const audit    = []

    for (const cluster of this.clusters) {
      const dec = this.decisions[cluster.id]
      const cid = cluster.cluster_id || String(cluster.id)

      for (const item of cluster.items) {
        const rid  = item._row_id
        const orig = String(item[descCol] || '')
        const nk   = item._normalized || normalize(orig)
        const pr   = this._resultMap[nk] || {}
        const pm   = pr.method || 'UNPROCESSED'
        const pc   = pr.confidence || ''

        let final, method, actionLabel
        if (dec?.action === 'merge') {
          final = dec.canonical_name; method = 'MANUAL_MERGE'
          actionLabel = orig !== final ? 'MERGED' : 'MERGED (no change)'
        } else if (cluster.is_auto && !dec) {
          final = cluster.suggested_name; method = pm; actionLabel = 'AUTO_MERGED'
        } else {
          final = orig; method = pm || 'KEPT'
          actionLabel = dec?.action === 'keep_all' ? 'KEPT'
                      : cluster.is_auto ? 'AUTO_MERGED' : 'UNREVIEWED'
        }

        rowFinal[rid] = { final, method, cid, conf: pc }
        audit.push({ row_id: rid + 2, no_po: item['No PO'] || '',
                     sumber_ru: item['Sumber RU'] || '', original: orig,
                     final, action: actionLabel, method, cluster_id: cid,
                     confidence: pc, similarity_score: cluster.avg_score,
                     timestamp: dec?.timestamp || '' })
      }
    }

    const finalRows = this.rawData.map(r => {
      const row  = { ...r }
      const info = rowFinal[r._row_id]
      row[cleanCol]       = info ? info.final : String(r[descCol] || '')
      row._method         = info?.method || 'UNPROCESSED'
      row._cluster_id     = info?.cid || ''
      row._confidence     = info?.conf || ''
      return row
    })
    return { finalRows, audit }
  }

  stats() {
    const ps = this._pipelineStats
    return {
      total_rows:     this.rawData.length,
      total_clusters: this.clusters.length,
      dup_clusters:   this.clusters.filter(c => c.is_duplicate).length,
      desc_col:       this.descColName,
      cache_hits:     ps.cache_hits     || 0,
      auto_merged:    ps.auto_merged    || 0,
      llm_clusters:   ps.llm_clusters   || 0,
      llm_batches:    ps.llm_batches    || 0,
    }
  }

  async cacheStats() { return this._pipeline.cache.stats() }
}

function hashStr(s) {
  let h = 0
  for (let i = 0; i < s.length; i++) h = Math.imul(31, h) + s.charCodeAt(i) | 0
  return h
}

module.exports = { POCleaner }
