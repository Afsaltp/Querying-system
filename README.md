# Querying System

This project provides two FastAPI backends for Order-to-Cash querying:

- `main.py` for the SAP-style dataset (`sap_ingest.py`, `sap_query_engine.py`, `sap_graph_api.py`)
- `main_sap.py` as an alternate entrypoint for the same SAP stack

## Quick Start

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. (Optional) Build the SAP SQLite database from JSONL data:

```bash
python sap_ingest.py --data-dir "<path-to-sap-o2c-data>" --db "data/otc.db"
```

3. Run API:

```bash
uvicorn main:app --reload --port 8000
```

4. Open the UI by serving `frontend.html` or opening it directly in a browser.

## Main Endpoints

- `POST /query`
- `POST /graph/node`
- `POST /graph/expand`
- `GET /graph/full`
- `GET /graph/broken-flows`
- `GET /health`

## Notes

- By default, the app looks for `data/otc.db` (or `otc.db` depending on entrypoint).
- Set `DB_PATH` to override database location.
- Set `GROQ_API_KEY` and/or `GEMINI_API_KEY` for natural-language-to-SQL generation.
