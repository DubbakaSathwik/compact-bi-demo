# Compact BI Demo

A conversational BI dashboard — ask questions in plain English, get instant SQL, charts and data insights from your CSV files. Powered by Google Gemini 2.5 Flash.

![dashboard](https://i.imgur.com/placeholder.png)

## Features

- Upload any CSV and auto-detect schema
- Ask natural-language questions → AI generates SQL → runs in-process
- Bar, line, pie chart and table visualisations (Chart.js)
- Multi-series bar charts for comparison queries  
- AI-generated demo queries with chart-type filters and count selector
- CSV data preview tab
- Live API key settings (no restart needed)
- Conversational history per session
- Zero npm dependencies — pure Node.js

---

## Quick Start (local)

### 1. Clone the repo
```bash
git clone https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git
cd YOUR_REPO_NAME
```

### 2. Get a free Gemini API key
Go to <https://aistudio.google.com/app/apikey> → **Create API key** → copy it.

### 3. Create your `.env` file
```bash
cp .env.example .env
```
Open `.env` and replace `PASTE_YOUR_KEY_HERE` with your key:
```
GEMINI_API_KEY=AIzaSy...
```

> **No key?** The app still works — it falls back to built-in demo queries and local SQL execution.

### 4. Run
```bash
node server.js
```
Open <http://localhost:3000> in your browser.

---

## Using the Dashboard

1. **Select a dataset** — pick one of the built-in CSVs or upload your own.
2. **Ask a question** — type anything like *"Show total revenue by region"* or *"What are the top 5 customers by spend?"*
3. **Click a demo query** — AI-generated suggestions appear on the right; filter by chart type and choose how many to generate.
4. **View results** — see the chart, the generated SQL, explanation and raw data side by side.
5. **Settings (⚙)** — update your Gemini API key live without restarting.

---

## Project Structure

```
compact-bi-demo/
├── server.js       # Node.js HTTP server, SQL engine, Gemini integration
├── index.html      # Single-page dashboard frontend
├── style.css       # All styles and animations
├── data/           # Sample CSV datasets
│   ├── sales_demo.csv
│   ├── customers_demo.csv
│   └── Customer Behaviour (Online vs Offline).csv
├── .env.example    # API key template (safe to share)
├── .env            # Your actual key — NOT committed to git
└── package.json
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Server | Node.js `http` (no frameworks) |
| AI | Google Gemini 2.5 Flash REST API |
| Charts | Chart.js (CDN) |
| Database | In-process CSV SQL engine |
| Frontend | Vanilla HTML/CSS/JS |

---

## Requirements

- Node.js 18 or later
- A free [Google Gemini API key](https://aistudio.google.com/app/apikey) *(optional — falls back gracefully)*

---

## Public URL Checklist

Before sharing this app with friends on a public URL:

1. Set production environment variables on your host:
	- `GEMINI_API_KEY`
	- `JWT_SECRET` (required, use a long random value)
	- `JWT_EXPIRES_IN` (optional, default: `8h`)
	- `PORT` (usually provided by host)
2. Keep `.env` private and never commit it.
3. Do not commit real user data in `data/users.json`.
4. Start with:
	- `npm start`

The server listens on `process.env.PORT` (fallback: `3000`), which is compatible with most PaaS providers.

---

## Deploy Options

### Option A: Railway / Render (recommended)

1. Import this GitHub repo.
2. Set the environment variables listed above.
3. Build command: `npm install`
4. Start command: `npm start`
5. Deploy and share the generated HTTPS URL.

### Option B: Temporary public sharing (quick demo)

Run locally, then expose with a tunnel:

```bash
cloudflared tunnel --url http://localhost:3000
```

or

```bash
ngrok http 3000
```
