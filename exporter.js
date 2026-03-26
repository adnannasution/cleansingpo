'use strict'
const XLSX = require('xlsx')

const METHOD_LABELS = {
  CACHE:'💾 Cache', FUZZY_AUTO:'⚡ Fuzzy Auto', LLM:'🤖 AI',
  MANUAL_MERGE:'✋ Manual', AUTO_MERGED:'⚡ Auto',
  KEPT:'🔒 Kept', UNREVIEWED:'⚠ Unreviewed', FALLBACK:'↩ Fallback', UNPROCESSED:'—'
}

function exportResults(finalRows, audit, originalHeaders, metadata) {
  const wb      = XLSX.utils.book_new()
  const descCol = metadata.desc_col || 'Deskripsi Material'
  const cleanCol = `${descCol} (Clean)`

  // ── Sheet 1: Data Clean ──────────────────────────────────────── //
  const skip   = new Set(['_row_id','_normalized','_method','_cluster_id','_confidence'])
  const outHdr = originalHeaders.filter(h => !skip.has(h))
  const dcIdx  = outHdr.indexOf(descCol)
  if (dcIdx >= 0) outHdr.splice(dcIdx + 1, 0, cleanCol)
  else outHdr.push(cleanCol)
  outHdr.push('Method', 'Cluster ID')

  const dataRows = finalRows.map(r => {
    const row = {}
    for (const h of outHdr) {
      if (h === 'Method')     row[h] = METHOD_LABELS[r._method] || r._method || ''
      else if (h === 'Cluster ID') row[h] = r._cluster_id || ''
      else row[h] = r[h] ?? ''
    }
    return row
  })

  const ws1 = XLSX.utils.json_to_sheet(dataRows, { header: outHdr })
  XLSX.utils.book_append_sheet(wb, ws1, 'Data Clean')

  // ── Sheet 2: Inventory Ready ─────────────────────────────────── //
  const invMap = {}
  for (const r of finalRows) {
    const key = `${r[cleanCol] || r[descCol] || ''}|||${r['Satuan'] || ''}`
    if (!invMap[key]) invMap[key] = { rows: [], desc: r[cleanCol] || r[descCol] || '', sat: r['Satuan'] || '' }
    invMap[key].rows.push(r)
  }

  const invRows = Object.values(invMap).map((g, i) => {
    const qty  = g.rows.reduce((s, r) => s + (parseFloat(r['Qty Order']) || 0), 0)
    const th   = g.rows.reduce((s, r) => s + (parseFloat(r['Total Harga (IDR)']) || 0), 0)
    const hvs  = g.rows.map(r => parseFloat(r['Harga Satuan (IDR)']) || 0).filter(v => v)
    const ah   = hvs.length ? hvs.reduce((a,b) => a+b, 0) / hvs.length : 0
    const origs = [...new Set(g.rows.map(r => String(r[descCol] || '')))]
    return {
      'No': i + 1,
      'Deskripsi Material (Standar)': g.desc,
      'Satuan': g.sat,
      'Total Qty': Math.round(qty * 100) / 100,
      'Harga Satuan Rata-rata (IDR)': Math.round(ah),
      'Total Harga (IDR)': Math.round(th),
      'Kategori': [...new Set(g.rows.map(r => r['Kategori'] || '').filter(Boolean))].join(' | '),
      'Sumber RU': [...new Set(g.rows.map(r => r['Sumber RU'] || '').filter(Boolean))].sort().join(' | '),
      'Jumlah PO': new Set(g.rows.map(r => r['No PO'] || '')).size,
      'Jumlah Baris': g.rows.length,
      'Status Merge': origs.length > 1 ? '✔ MERGED' : '—',
    }
  })
  XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(invRows), 'Inventory Ready')

  // ── Sheet 3: Audit Trail ─────────────────────────────────────── //
  const auditRows = audit.map(e => ({
    'Excel Row':            e.row_id,
    'No PO':                e.no_po,
    'Sumber RU':            e.sumber_ru,
    'Deskripsi Original':   e.original,
    'Deskripsi Final':      e.final,
    'Aksi':                 e.action,
    'Method':               METHOD_LABELS[e.method] || e.method,
    'Cluster ID':           e.cluster_id,
    'Confidence':           e.confidence,
    'Similarity Score':     e.similarity_score,
    'Timestamp':            e.timestamp,
  }))
  XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(auditRows), 'Audit Trail')

  // ── Sheet 4: Summary ─────────────────────────────────────────── //
  const sumRows = [
    ['Tanggal Cleansing', new Date().toLocaleString('id-ID')],
    ['File Sumber',       metadata.source_file || ''],
    ['Kolom Duplikat',    metadata.desc_col || ''],
    ['Threshold',         `${metadata.threshold || 75}%`],
    ['',''],
    ['Total Baris',       metadata.total_rows || 0],
    ['Layer 2 Cache Hit', metadata.cache_hits || 0],
    ['Layer 3 Auto-merge',metadata.auto_merged || 0],
    ['Layer 4 AI Cluster',metadata.llm_clusters || 0],
    ['Layer 4 API Calls', metadata.llm_batches || 0],
    ['',''],
    ['Item Unik Inventory', invRows.length],
    ['Total Cluster',     metadata.total_clusters || 0],
    ['Duplikat',          metadata.dup_clusters || 0],
  ]
  XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(sumRows), 'Summary')

  return XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' })
}

module.exports = { exportResults }
