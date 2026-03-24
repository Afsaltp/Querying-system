# How to Connect Your SAP Dataset

## Your Data Structure (already understood)
```
sap-o2c-data/
  sales_order_headers/        → 100 records
  sales_order_items/          → 167 records
  outbound_delivery_headers/  → 86 records
  billing_document_cancellations/ → 80 records
  payments_accounts_receivable/   → 120 records
  journal_entry_items_accounts_receivable/ → 123 records
  business_partners/          → 8 records
  customer_company_assignments/   → 8 records
  customer_sales_area_assignments/ → 28 records
  product_descriptions/       → 69 records
  product_plants/             → 200 records
  product_storage_locations/  → 16,723 records
  plants/                     → 44 records
```

## Step 1 — Place Your Data

Put your `sap-o2c-data/` folder inside the project's `data/` directory:

```
otc-graph/
  data/
    sap-o2c-data/       ← your unzipped dataset goes here
      sales_order_headers/
      billing_document_cancellations/
      ... etc
```

## Step 2 — Run the Ingestion Pipeline

```bash
cd otc-graph/backend
python pipeline/sap_ingest.py \
  --data-dir ../data/sap-o2c-data \
  --db ../data/otc.db
```

Expected output:
```
✓   100 sales_order_headers
✓   167 sales_order_items
✓    86 outbound_delivery_headers
✓    80 billing_documents
✓   120 payments_ar
✓   123 journal_entry_items_ar
...
✓  8161 edges built
✅ Database ready: data/otc.db
```

## Step 3 — Set LLM API Key (FREE)

Get a free Groq key at https://console.groq.com

```bash
export GROQ_API_KEY=gsk_your_key_here
# or for Gemini:
export GEMINI_API_KEY=your_key
```

## Step 4 — Start the Backend

```bash
cd otc-graph/backend
pip install fastapi uvicorn httpx --break-system-packages
uvicorn main:app --reload --port 8000
```

## Step 5 — Open the Frontend

```bash
open otc-graph/frontend/index.html
# or: python -m http.server 3000 (in frontend/)
```

## Example Queries That Work With Your Data

| Query | What it does |
|-------|-------------|
| `Trace full flow of billing document 90504274` | Shows billing → journal 9400000275 → customer 320000083 |
| `Find all cancelled billing documents` | Lists billingDocumentIsCancelled=1 records |
| `Which orders have no billing?` | 100 sales orders with no linked billing doc |
| `Show payment summary by customer` | Groups payments_ar by customer |
| `What is the journal entry for billing 90504274?` | Returns accountingDocument 9400000275 |
| `Find billing documents for customer 320000083` | All billing docs for soldToParty |
| `Show materials in sales order 740506` | Joins sales_order_items → product_descriptions |

## Key SAP Field Mappings (important!)

| SAP Term | Table | Column |
|----------|-------|--------|
| Sales Order | sales_order_headers | salesOrder |
| Billing Doc | billing_documents | billingDocument |
| FI/Journal Doc | journal_entry_items_ar | accountingDocument |
| Customer | billing_documents | soldToParty |
| Customer Master | business_partners | businessPartner |
| Payment | payments_ar | accountingDocument + item |
| Material | sales_order_items | material |

## If You Get New Data Files

Just re-run `sap_ingest.py` — it uses `INSERT OR REPLACE` so it's idempotent.
New part-*.jsonl files in any folder are automatically picked up.
