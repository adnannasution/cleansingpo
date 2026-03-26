const express = require("express");
const fetch = require("node-fetch");
const path = require("path");

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static("public"));

const PYTHON_API = process.env.PYTHON_API || "http://localhost:5001";

// Health check
app.get("/health", (req, res) => {
    res.json({ ok: true });
});

// Start scan → kirim ke Python
app.post("/scan-start", async (req, res) => {
    try {
        const r = await fetch(`${PYTHON_API}/scan-start`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(req.body)
        });
        const data = await r.json();
        res.json(data);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Cek progress
app.get("/scan-status", async (req, res) => {
    try {
        const r = await fetch(`${PYTHON_API}/scan-status?job_id=${req.query.job_id}`);
        const data = await r.json();
        res.json(data);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Upload file → Python
app.post("/upload", async (req, res) => {
    try {
        const r = await fetch(`${PYTHON_API}/upload`, {
            method: "POST",
            body: req
        });
        const data = await r.json();
        res.json(data);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log("Node server running on port", PORT);
});