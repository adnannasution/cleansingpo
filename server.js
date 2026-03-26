const express = require("express");
const fetch = require("node-fetch");

const app = express();
app.use(express.json());
app.use(express.static("public"));

const PYTHON_API = process.env.PYTHON_API || "http://127.0.0.1:5001";

app.get("/health", (req, res) => {
    res.status(200).send("OK");
});

app.post("/scan-start", async (req, res) => {
    const r = await fetch(`${PYTHON_API}/scan-start`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(req.body)
    });
    const data = await r.json();
    res.json(data);
});

app.get("/scan-status", async (req, res) => {
    const r = await fetch(`${PYTHON_API}/scan-status?job_id=${req.query.job_id}`);
    const data = await r.json();
    res.json(data);
});

// PENTING BANGET
const PORT = process.env.PORT || 3000;
app.listen(PORT, "0.0.0.0", () => {
    console.log("Node running on port", PORT);
});