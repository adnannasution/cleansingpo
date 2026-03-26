'use strict'
const express = require('express')
const multer  = require('multer')
const XLSX    = require('xlsx')
const path    = require('path')
const { v4: uuidv4 } = require('uuid')
const { POCleaner }  = require('./cleaner')
const { exportResults } = require('./exporter')

const app    = express()
const PORT   = process.env.PORT || 3000
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 20 * 1024 * 1024 } })

app.use(express.json())
app.use(express.static(path.join(__dirname, 'public')))

// ── State ──────────────────────────────────────────────────────── //
const sessions = {}   // session_id -> POCleaner
const jobs     = {}   // job_id -> {status, pct, msg, result, error}

function getSession(sid) {
  if (!sessions[sid]) sessions[sid] = new POCleaner()
  return sessions[sid]
}

// ── Health ─────────────────────────────────────────────────────── //
app.get('/health', (_, res) => res.json({ ok: true }))

// ── Upload ─────────────────────────────────────────────────────── //
app.post('/api/upload', upload.single('file'), (req, res) => {
  const sid = req.body.session_id || 'default'
  if (!req.file) return res.status(400).json({ ok: false, error: 'No file' })
  try {
    const wb     = XLSX.read(req.file.buffer, { type: 'buffer' })
    const result = getSession(sid).loadExcel(wb)
    if (!result.ok) return res.status(400).json(result)
    res.json(result)
  } catch(e) { res.status(500).json({ ok: false, error: e.message }) }
})

// ── Set column ─────────────────────────────────────────────────── //
app.post('/api/set-column', (req, res) => {
  const { session_id: sid = 'default', column } = req.body
  res.json(getSession(sid).setDescColumn(column))
})

// ── Start scan (returns job_id, runs in background) ────────────── //
app.post('/api/scan-start', async (req, res) => {
  const { session_id: sid = 'default', api_key = '',
          skip_llm = true, threshold = 75 } = req.body
  const jobId   = uuidv4()
  jobs[jobId]   = { status: 'running', pct: 0, msg: 'Memulai...', result: null, error: null }
  res.json({ ok: true, job_id: jobId })

  // Run async
  const cleaner   = getSession(sid)
  const apiConfig = api_key
    ? { base_url: 'https://ai.dinoiki.com/v1', api_key, model: 'claude-sonnet-4-6' }
    : {}

  const prog = (pct, msg) => {
    jobs[jobId].pct = Math.round(pct * 100)
    jobs[jobId].msg = msg
  }

  cleaner.runPipeline(apiConfig, parseInt(threshold), prog, skip_llm || !api_key)
    .then(() => {
      const s    = cleaner.stats()
      const data = cleaner.clusters.map(c => ({
        id:             c.id,
        cluster_id:     c.cluster_id,
        orig_descs:     c.orig_descs,
        suggested_name: c.suggested_name,
        avg_score:      c.avg_score,
        is_duplicate:   c.is_duplicate,
        is_auto:        c.is_auto,
        method:         c.method,
        confidence:     c.confidence,
        ai_result:      c.ai_result,
        items_count:    c.items_count,
        ru_list:        c.ru_list,
      }))
      jobs[jobId] = { status: 'done', pct: 100, msg: 'Selesai.',
                      result: { stats: s, clusters: data }, error: null }
    })
    .catch(e => {
      jobs[jobId] = { status: 'error', pct: 0, msg: e.message,
                      result: null, error: e.stack }
    })
})

// ── Poll job status ─────────────────────────────────────────────── //
app.get('/api/scan-status', (req, res) => {
  const job = jobs[req.query.job_id]
  if (!job) return res.status(404).json({ status: 'not_found' })
  res.json(job)
})

// ── Export ─────────────────────────────────────────────────────── //
app.post('/api/export', (req, res) => {
  const { session_id: sid = 'default', decisions = {} } = req.body
  const cleaner = getSession(sid)

  for (const [cidStr, dec] of Object.entries(decisions)) {
    try { cleaner.saveDecision(parseInt(cidStr), dec.action, dec.canonical_name) }
    catch(_) {}
  }

  try {
    const { finalRows, audit } = cleaner.applyDecisions()
    const s = cleaner.stats()
    const buf = exportResults(finalRows, audit, cleaner.headerRow, {
      ...s, source_file: 'web_upload.xlsx', threshold: 75
    })
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    res.setHeader('Content-Disposition', `attachment; filename="PO_Clean_${Date.now()}.xlsx"`)
    res.send(buf)
  } catch(e) { res.status(500).json({ ok: false, error: e.message }) }
})

// ── Cache stats ─────────────────────────────────────────────────── //
app.get('/api/cache-stats', async (req, res) => {
  try { res.json(await getSession(req.query.session_id || 'default').cacheStats()) }
  catch(_) { res.json({ total: 0 }) }
})

app.listen(PORT, () => console.log(`[Server] port ${PORT}`))
