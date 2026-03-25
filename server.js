const express = require('express')
const { createProxyMiddleware } = require('http-proxy-middleware')
const { spawn } = require('child_process')
const path = require('path')
const app = express()

const PORT        = process.env.PORT || 3000
const PYTHON_PORT = process.env.PYTHON_PORT || 5001
const PYTHON_URL  = `http://127.0.0.1:${PYTHON_PORT}`

app.use(express.json())
app.use(express.static(path.join(__dirname, 'public')))

// ── Start Python worker ───────────────────────────────────────────── //
function startPython() {
  const py = spawn('python3', [path.join(__dirname, 'python/worker.py')], {
    env: { ...process.env, PYTHON_PORT: String(PYTHON_PORT) },
    stdio: 'inherit',
  })
  py.on('close', code => {
    if (code !== 0) {
      console.error(`[Python] exited ${code}, restarting in 3s`)
      setTimeout(startPython, 3000)
    }
  })
  return py
}

// ── Wait for Python to be ready ───────────────────────────────────── //
async function waitForPython(retries = 30) {
  for (let i = 0; i < retries; i++) {
    try {
      const r = await fetch(`${PYTHON_URL}/health`)
      if (r.ok) { console.log('[Python] ready'); return }
    } catch (_) {}
    await new Promise(r => setTimeout(r, 1000))
  }
  throw new Error('Python worker failed to start')
}

// ── Proxy all /py/* to Python worker ─────────────────────────────── //
app.use('/py', createProxyMiddleware({
  target: PYTHON_URL,
  changeOrigin: true,
  pathRewrite: { '^/py': '' },
  on: {
    error: (err, req, res) => {
      res.status(502).json({ ok: false, error: 'Python worker unavailable' })
    }
  }
}))

// ── Health check ──────────────────────────────────────────────────── //
app.get('/health', (_, res) => res.json({ ok: true }))

// ── Start ─────────────────────────────────────────────────────────── //
;(async () => {
  startPython()
  await waitForPython()
  app.listen(PORT, () => console.log(`[Node] listening on port ${PORT}`))
})()
