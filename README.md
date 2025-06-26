# ✈️ Airline Analytics Showcase

This project was designed to showcase the final layer of a full-stack data engineering pipeline using DBT + PostgreSQL + Python. The goal is to simulate a realistic airline operations P&L modeling use case and share results in formats accessible to recruiters, collaborators, or friends.

## Features
- Connects to DBT gold models via Postgres 
- Exports tables to:
  - HTML for web sharing (PLACEHOLDER, TO BE FIXED)
  - Excel for recruiters
  - CSV for GitHub-friendly diffs
- Optional Streamlit dashboard

## Setup

```bash
pip install -r requirements.txt
python scripts/export_gold_tables.py
```

## Next Steps & Enhancements
- Add a Streamlit dashboard to visually explore key profitability metrics
- Include aggregate KPIs (e.g. avg margin by route, delay costs) in export files
- Improve formatting and styling of HTML outputs for personal portfolio sharing

## Folder Structure
```
airline-analytics-layer/
├── outputs/                  # Excel, CSV, and HTML table exports
├── scripts/
│   └── export_gold_tables.py # Main export script
├── requirements.txt          # Python dependencies
└── README.md                 # This file
```