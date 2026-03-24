<<<<<<< HEAD
# ⬡ OTC Graph Intelligence System

> Graph-based data modeling + LLM-powered query interface for Order-to-Cash business flows.

---

## 🧠 SYSTEM ARCHITECTURE

### Technology Decisions (Justified)

| Layer | Choice | Why |
|-------|--------|-----|
| **Backend** | FastAPI (Python) | Async native, Pydantic validation, auto OpenAPI docs, fast iteration |
| **Database** | SQLite + Adjacency Table | Zero-dependency, graph traversal via recursive CTEs, deployable anywhere |
| **Frontend** | Vanilla JS + D3.js | No build step, loads in browser, D3 force graph is best-in-class |
| **LLM** | Groq/Llama-3.1-8B → Gemini Flash fallback | Free tier, fast inference, good SQL generation |

### Why Hybrid (Not Pure Neo4j)?

```
PURE NEO4J:          PURE RELATIONAL:      OUR HYBRID:
✓ Graph algorithms   ✓ Fast aggregations   ✓ Graph traversal (recursive CTE)
✗ Paid tiers         ✓ SQL familiarity      ✓ Fast SQL aggregations  
✗ Docker required    ✗ Multi-hop is JOIN   ✓ Free, no Docker
✗ Cypher complexity  ✗ hell                ✓ Deployable everywhere
```

### End-to-End Query Flow

```
User: "Trace full flow of billing document BILL00001"
  │
  ▼
[GUARDRAIL] Topic check: "billing" keyword found → proceed
  │
  ▼
[LLM] Groq/Llama-3.1: Schema injected → generates SQL
  │
  ▼
[VALIDATE] Safety check (no DROP/DELETE) + dry-run with LIMIT 0
  │
  ▼
[EXECUTE] SQLite query → returns rows
  │
  ▼
[GROUND] LLM sees ONLY the returned rows → generates answer
  │
  ▼
[HIGHLIGHT] Extract node IDs from results → frontend highlights graph
  │
  ▼
Response: "Billing BILL00001 for ₹45,000 was created on 2025-04-03, 
           linked to Order ORD00001 (DELIVERED), settled by payment 
           PAY00001 via BANK_TRANSFER."
```

---

## 🧩 GRAPH DATA MODEL

```
┌─────────────┐  PLACED_BY   ┌──────────────┐
│   ORDER     │─────────────▶│   CUSTOMER   │
│  order_id   │              │  customer_id │
│  status     │              │  name        │
│  amount     │              │  segment     │
└─────────────┘              └──────────────┘
      │
      │ FULFILLED_BY
      ▼
┌─────────────┐  INVOICED_BY  ┌──────────────┐
│  DELIVERY   │──────────────▶│   BILLING    │
│ delivery_id │               │  billing_id  │
│  status     │               │  amount      │
│  carrier    │               │  status      │
└─────────────┘               └──────────────┘
                                     │
      │ BILLED_AS                    │ SETTLED_BY
      └──────────────────────────────▶│
                                ┌──────────────┐
                                │   PAYMENT    │
                                │  payment_id  │
                                │  method      │
                                │  amount      │
                                └──────────────┘

Additional nodes: Product, Address, JournalEntry
```

### Missing Link Strategy
- FK points to non-existent record → **log warning, skip edge** (not phantom nodes)
- Broken flows become visible via `/graph/broken-flows` endpoint
- Severity classification: CRITICAL (revenue leak) > WARNING (overdue) > INFO (new orders)

---

## 📁 PROJECT STRUCTURE

```
otc-graph/
├── backend/
│   ├── main.py                 # FastAPI app + route definitions
│   ├── graph/
│   │   └── graph_store.py      # SQLite hybrid store + recursive CTE traversal
│   ├── api/
│   │   ├── query_engine.py     # NL→SQL pipeline + guardrails + grounding
│   │   └── graph_api.py        # Node ops, expansion, broken flow detection
│   └── pipeline/
│       └── ingest.py           # CSV→graph ETL pipeline
├── frontend/
│   └── index.html              # D3 force graph + chat UI (single file, no build)
├── data/                       # CSV files go here
├── requirements.txt
└── README.md
```

---

## 🚀 SETUP

### 1. Install Dependencies

```bash
cd backend
pip install fastapi uvicorn httpx pandas python-dotenv
```

### 2. Configure LLM API Key

```bash
# Get free API key from: https://console.groq.com
export GROQ_API_KEY=your_key_here

# Optional fallback
export GEMINI_API_KEY=your_gemini_key
```

### 3. Generate Sample Data + Ingest

```bash
# Generate realistic sample data (100 orders, 50 customers, etc.)
python pipeline/ingest.py --generate-sample --data-dir ../data/ --db ../data/otc.db

# Or load your own CSVs:
# data/customers.csv, orders.csv, deliveries.csv, billing_documents.csv, payments.csv
python pipeline/ingest.py --data-dir ../data/ --db ../data/otc.db
```

**Expected CSV columns:**
- `customers.csv`: customer_id, name, email, segment, country, created_at
- `orders.csv`: order_id, customer_id, order_date, status, total_amount, currency
- `deliveries.csv`: delivery_id, order_id, ship_date, actual_delivery_date, status, carrier
- `billing_documents.csv`: billing_id, order_id, delivery_id, customer_id, invoice_date, amount, status
- `payments.csv`: payment_id, billing_id, customer_id, payment_date, amount, method

### 4. Start Backend

```bash
cd backend
uvicorn main:app --reload --port 8000
```

### 5. Open Frontend

```bash
# Simply open in browser - no build required
open frontend/index.html

# Or serve with Python
cd frontend && python -m http.server 3000
```

---

## 💬 EXAMPLE QUERIES

### Business Intelligence
```
"Which products have highest billing count?"
→ SQL JOIN across products, order_items, billing_documents
→ Ranked table + bar chart suggestion

"Find customers with most orders this year"
→ GROUP BY customer with ORDER BY order_count DESC

"What is the average payment delay for enterprise customers?"
→ DATE diff between invoice_date and payment_date
```

### Flow Tracing (Graph Traversal)
```
"Trace full flow of billing document BILL00001"
→ Multi-table JOIN: billing → order → delivery → payment → customer

"Show me all documents linked to order ORD00023"
→ Graph expand from order node, depth=2

"91150187 - Find the journal entry number linked to this"
→ JOIN billing_documents ON reference_document + journal_entries
```

### Broken Flow Detection
```
"Find incomplete order flows"
→ LEFT JOIN orders → deliveries WHERE delivery IS NULL AND status != CANCELLED

"Which invoices are overdue with no payment?"
→ status = OVERDUE AND payment_id IS NULL

"Show delivered orders with no billing"
→ CRITICAL flow: revenue leak detection
```

---

## 🛡️ GUARDRAILS

```python
GUARDRAIL #1: Topic Filter
  "What is the weather today?" → REJECTED (no OTC keywords)
  "Show overdue invoices" → ALLOWED

GUARDRAIL #2: SQL Safety  
  Generated SQL containing DROP/DELETE → REJECTED before execution
  Only SELECT/WITH queries allowed

GUARDRAIL #3: Dry-Run Validation
  SQL with invalid column names → caught with LIMIT 0 test run

GUARDRAIL #4: Response Grounding
  LLM response built FROM query results only
  System prompt: "Do NOT add information not in the results"
  Empty results → "No matching records found" (not hallucinated data)
```

---

## ⚙️ API REFERENCE

```
POST /query
  Body: { "query": "Find overdue invoices", "session_id": "abc123" }
  Returns: { answer, sql, results, highlighted_nodes, status }

POST /graph/node  
  Body: { "node_id": "BILL00001", "node_type": "billing" }
  Returns: { attributes, edges, edge_count }

POST /graph/expand
  Body: { "node_id": "ORD00001", "node_type": "order", "depth": 2 }
  Returns: { nodes, edges } for subgraph

GET /graph/full
  Returns: Complete graph (or clustered view if >500 nodes)

GET /graph/broken-flows
  Returns: { critical[], warnings[], info[], summary }

GET /health
  Returns: { status, nodes: {orders, deliveries, ...} }
```

---

## 📊 ADVANCED FEATURES IMPLEMENTED

1. **Graph Traversal**: Recursive CTE in SQLite enables multi-hop reasoning without Neo4j
2. **Broken Flow Detection**: Automated severity-classified gap analysis across the OTC chain
3. **Node Highlighting**: Query results drive visual highlighting in the D3 graph
4. **Conversation Memory**: Per-session history (last 10 turns) for context-aware follow-ups
5. **Semantic Grounding**: LLM sees only actual query results → zero hallucination

---

## 🔧 SCALABILITY NOTES

- **>10K nodes**: Switch from detailed to clustered graph view (automatic at 500)
- **>1M rows**: Add PostgreSQL with pg_trgm for text search, keep adjacency table approach
- **Multi-tenant**: Add org_id column to all tables + RLS policies
- **Query caching**: Redis with TTL=300s for repeated identical queries
- **LLM costs**: Cache SQL generation results by normalized question hash

---

## 🔑 FREE LLM TIERS

| Provider | Model | Free Tier | Speed |
|----------|-------|-----------|-------|
| **Groq** | llama-3.1-8b-instant | 14,400 req/day | Very fast |
| **Google Gemini** | gemini-1.5-flash | 1,500 req/day | Fast |
| **OpenRouter** | mistral-7b | Limited | Medium |

Get Groq key: https://console.groq.com (recommended - fastest, most generous free tier)
#   Q u e r y i n g - s y s t e m  
 
=======
# Querying-system
>>>>>>> 7bd959320217013688af98ee8a38a8df63aa3c31
#   Q u e r y i n g - s y s t e m  
 