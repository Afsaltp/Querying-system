"""
SAP-aware Query Engine — FIXED (flat imports, no subfolders)
Natural Language → SQL → Grounded Answer
"""
import os, re, json, sqlite3, httpx
from typing import Optional

SAP_SYSTEM_PROMPT = """You are a precise SQL query generator for a SAP Order-to-Cash (OTC) system.

DATABASE SCHEMA (SQLite — use EXACT column names shown):
{schema}

GRAPH EDGES TABLE:
  edges(from_type, from_id, relationship, to_type, to_id)
  Relationships: PLACED_BY, BILLED_TO, POSTED_AS, REFERENCES_BILLING,
                 CLEARS, PAID_BY, CONTAINS_MATERIAL, HAS_DELIVERY, STORED_AT

KEY SAP RULES — READ CAREFULLY:
- "soldToParty" = customer ID in sales_order_headers and billing_documents
- "businessPartner" = customer ID in business_partners table
- journal.referenceDocument = billing.billingDocument  (billing ↔ journal link)
- journal.accountingDocument = billing.accountingDocument  (same link, different angle)
- payment.accountingDocument = journal.accountingDocument  (payment ↔ journal link)
- "accountingDocument" is the FI/journal doc number e.g. 9400000275
- "billingDocument" is the SD billing doc number e.g. 90504274
- "salesOrder" is the SD sales order number e.g. 740506
- Always add LIMIT 100. Use table aliases. SELECT only — never INSERT/UPDATE/DELETE.
- If the question cannot be answered from this schema output exactly: UNRESOLVABLE

FEW-SHOT EXAMPLES:

Q: Which products have highest billing count?
SQL: SELECT i.material, pd.productDescription,
  COUNT(DISTINCT b.billingDocument) as billing_count,
  SUM(b.totalNetAmount) as total_billed
FROM sales_order_items i
JOIN billing_documents b ON i.salesOrder = b.soldToParty
LEFT JOIN product_descriptions pd ON i.material = pd.product AND pd.language = 'EN'
GROUP BY i.material, pd.productDescription
ORDER BY billing_count DESC LIMIT 10;

Q: Find the journal entry number for billing document 90504274
SQL: SELECT b.billingDocument, b.accountingDocument as journalEntryNumber,
  b.fiscalYear, b.companyCode, b.totalNetAmount,
  j.glAccount, j.amountInTransactionCurrency, j.postingDate
FROM billing_documents b
LEFT JOIN journal_entry_items_ar j ON b.accountingDocument = j.accountingDocument
WHERE b.billingDocument = '90504274' LIMIT 20;

Q: Trace full flow for billing document 90504274
SQL: SELECT b.billingDocument, b.billingDocumentType, b.totalNetAmount,
  b.billingDocumentDate, b.billingDocumentIsCancelled,
  b.accountingDocument as journalDoc,
  j.glAccount, j.amountInTransactionCurrency as journalAmount,
  j.clearingAccountingDocument, j.clearingDate,
  p.amountInTransactionCurrency as paymentAmount, p.clearingDate as paymentDate,
  bp.businessPartnerFullName as customerName
FROM billing_documents b
LEFT JOIN journal_entry_items_ar j ON b.accountingDocument = j.accountingDocument
LEFT JOIN payments_ar p ON j.accountingDocument = p.accountingDocument
LEFT JOIN business_partners bp ON b.soldToParty = bp.businessPartner
WHERE b.billingDocument = '90504274' LIMIT 100;

Q: Find incomplete order flows (orders with no billing)
SQL: SELECT s.salesOrder, s.soldToParty, s.totalNetAmount, s.creationDate,
  s.overallDeliveryStatus, bp.businessPartnerFullName as customerName
FROM sales_order_headers s
LEFT JOIN billing_documents b ON s.salesOrder = b.soldToParty
LEFT JOIN business_partners bp ON s.soldToParty = bp.businessPartner
WHERE b.billingDocument IS NULL
ORDER BY s.creationDate DESC LIMIT 100;

Q: Show all billing documents for customer 320000083
SQL: SELECT b.billingDocument, b.billingDocumentType, b.totalNetAmount,
  b.transactionCurrency, b.billingDocumentDate, b.billingDocumentIsCancelled,
  b.accountingDocument
FROM billing_documents b
WHERE b.soldToParty = '320000083'
ORDER BY b.billingDocumentDate DESC LIMIT 100;

Q: Find cancelled billing documents
SQL: SELECT billingDocument, billingDocumentType, totalNetAmount,
  transactionCurrency, creationDate, cancelledBillingDocument, soldToParty
FROM billing_documents
WHERE billingDocumentIsCancelled = 1
ORDER BY creationDate DESC LIMIT 100;

Q: Show payment summary by customer
SQL: SELECT bp.businessPartnerFullName, p.customer,
  COUNT(*) as payment_count,
  SUM(p.amountInTransactionCurrency) as total_paid,
  p.transactionCurrency
FROM payments_ar p
LEFT JOIN business_partners bp ON p.customer = bp.businessPartner
GROUP BY p.customer, bp.businessPartnerFullName, p.transactionCurrency
ORDER BY total_paid DESC LIMIT 20;

Q: List materials in sales orders with descriptions
SQL: SELECT DISTINCT i.material, pd.productDescription, i.materialGroup,
  i.productionPlant, COUNT(DISTINCT i.salesOrder) as order_count,
  SUM(i.netAmount) as total_value
FROM sales_order_items i
LEFT JOIN product_descriptions pd ON i.material = pd.product AND pd.language = 'EN'
GROUP BY i.material, pd.productDescription, i.materialGroup, i.productionPlant
ORDER BY order_count DESC LIMIT 50;

Q: {question}
SQL:"""

RESPONSE_PROMPT = """You are a SAP business analyst for Order-to-Cash.
User asked: "{question}"
Database returned: {results}

Write a clear, concise answer (2-4 sentences) using ONLY the data above.
- Reference specific SAP document numbers, amounts, dates from the results
- Do NOT invent any data not present in the results
- If results are empty, say "No matching records found"
- Mention INR currency where relevant
"""

OTC_KEYWORDS = [
    'order', 'billing', 'bill', 'invoice', 'payment', 'delivery', 'customer',
    'material', 'product', 'journal', 'accounting', 'document', 'plant',
    'fiscal', 'cancel', 'posting', 'gl', 'amount', 'currency', 'inr',
    'how many', 'which', 'find', 'show', 'list', 'trace', 'flow', 'status',
    'what', 'when', 'who', 'total', 'sum', 'count', 'top', 'overdue',
    'pending', 'unpaid', 'paid', 'cancelled', 'incomplete', 'missing',
    'link', 'reference', '740', '905', '940', '320', '310',
]


class SAPQueryEngine:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.groq_key = os.getenv("GROQ_API_KEY", "")
        self.gemini_key = os.getenv("GEMINI_API_KEY", "")
        self.session_memory: dict = {}
        self._schema_cache: Optional[str] = None

    # ── SCHEMA ────────────────────────────────────────────────────────────────
    def _get_schema(self) -> str:
        if self._schema_cache:
            return self._schema_cache
        tables = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name != 'edges' AND name != 'sqlite_sequence' ORDER BY name"
        ).fetchall()
        lines = []
        for (t,) in tables:
            cols = self.conn.execute(f"PRAGMA table_info({t})").fetchall()
            col_str = ", ".join(c[1] for c in cols)
            lines.append(f"  {t}({col_str})")
        self._schema_cache = "\n".join(lines)
        return self._schema_cache

    # ── GUARDRAILS ────────────────────────────────────────────────────────────
    def _is_otc_query(self, q: str) -> bool:
        ql = q.lower()
        return any(kw in ql for kw in OTC_KEYWORDS)

    def _is_safe_sql(self, sql: str) -> tuple:
        su = sql.upper()
        for kw in ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'CREATE', 'ALTER', 'TRUNCATE']:
            if re.search(rf'\b{kw}\b', su):
                return False, f"Blocked: {kw}"
        if not re.match(r'^\s*(SELECT|WITH)', sql.strip(), re.IGNORECASE):
            return False, "Only SELECT allowed"
        return True, "ok"

    def _dry_run(self, sql: str) -> bool:
        try:
            test = re.sub(r'LIMIT\s+\d+', 'LIMIT 0', sql, flags=re.IGNORECASE)
            if 'LIMIT' not in test.upper():
                test += ' LIMIT 0'
            self.conn.execute(test).fetchall()
            return True
        except:
            return False

    def _clean_sql(self, raw: str) -> str:
        s = raw.strip()
        s = re.sub(r'^```sql\s*', '', s, flags=re.IGNORECASE)
        s = re.sub(r'^```\s*', '', s)
        s = re.sub(r'```\s*$', '', s)
        s = re.sub(r'^SQL:\s*', '', s, flags=re.IGNORECASE)
        return s.strip()

    def _execute(self, sql: str) -> list:
        rows = self.conn.execute(sql).fetchall()
        return [dict(r) for r in rows]

    def _extract_highlights(self, results: list) -> list:
        patterns = {
            'sales_order':      r'^\d{6}$',
            'billing_document': r'^\d{8}$',
            'business_partner': r'^\d{9}$',
            'journal_entry':    r'^94\d{7,}',
            'material':         r'^[A-Z]\d{10,}',
        }
        seen, out = set(), []
        for row in results[:30]:
            for val in row.values():
                if not isinstance(val, str):
                    continue
                v = val.strip()
                for ntype, pat in patterns.items():
                    if re.match(pat, v) and v not in seen:
                        seen.add(v)
                        out.append({"type": ntype, "id": v})
        return out

    # ── LLM CALLS ─────────────────────────────────────────────────────────────
    async def _call_groq(self, prompt: str, system: str = "You are a precise SQL generator. Output only SQL.") -> str:
        headers = {
            "Authorization": f"Bearer {self.groq_key}",
            "Content-Type": "application/json"
        }
        body = {
            "model": "llama-3.1-8b-instant",
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.0,
            "max_tokens": 600,
        }
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers=headers, json=body
            )
            r.raise_for_status()
            return r.json()["choices"][0]["message"]["content"].strip()

    async def _call_gemini(self, prompt: str) -> str:
        url = (f"https://generativelanguage.googleapis.com/v1beta/models/"
               f"gemini-1.5-flash:generateContent?key={self.gemini_key}")
        body = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.0, "maxOutputTokens": 600}
        }
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.post(url, json=body)
            r.raise_for_status()
            return r.json()["candidates"][0]["content"]["parts"][0]["text"].strip()

    async def _llm(self, prompt: str, system: str = "You are a precise SQL generator. Output only SQL.") -> str:
        if self.groq_key:
            try:
                return await self._call_groq(prompt, system)
            except Exception as e:
                print(f"Groq error: {e}")
        if self.gemini_key:
            return await self._call_gemini(prompt)
        raise RuntimeError("No LLM key found. Set GROQ_API_KEY environment variable.\nGet a free key at: https://console.groq.com")

    # ── MAIN PIPELINE ─────────────────────────────────────────────────────────
    async def process(self, question: str, session_id: str = "default") -> dict:
        # 1. Topic guard
        if not self._is_otc_query(question):
            return {
                "answer": "I only answer questions about the SAP Order-to-Cash dataset (orders, billing, payments, journal entries, customers, materials).",
                "sql": None, "results": [], "highlighted_nodes": [], "status": "rejected"
            }

        # 2. Generate SQL
        schema = self._get_schema()
        prompt = SAP_SYSTEM_PROMPT.format(schema=schema, question=question)
        try:
            raw = await self._llm(prompt)
        except Exception as e:
            return {
                "answer": str(e),
                "sql": None, "results": [], "highlighted_nodes": [], "status": "error"
            }

        if "UNRESOLVABLE" in raw:
            return {
                "answer": "This question cannot be answered from the available SAP dataset.",
                "sql": None, "results": [], "highlighted_nodes": [], "status": "unresolvable"
            }

        # 3. Clean + validate SQL
        sql = self._clean_sql(raw)
        ok, reason = self._is_safe_sql(sql)
        if not ok:
            return {"answer": f"Query rejected: {reason}", "sql": sql, "results": [], "status": "rejected"}

        if not self._dry_run(sql):
            return {
                "answer": "Could not validate the generated query. Try rephrasing your question.",
                "sql": sql, "results": [], "highlighted_nodes": [], "status": "invalid_sql"
            }

        # 4. Execute
        try:
            results = self._execute(sql)
        except Exception as e:
            return {"answer": f"Query error: {e}", "sql": sql, "results": [], "status": "error"}

        # 5. Generate grounded response
        resp_prompt = RESPONSE_PROMPT.format(
            question=question,
            results=json.dumps(results[:20], indent=2, default=str)
        )
        try:
            answer = await self._llm(resp_prompt, system="You are a concise SAP business analyst.")
        except:
            answer = (f"Found {len(results)} record(s)." if results else "No matching records found.")

        # 6. Extract node highlights for graph
        highlights = self._extract_highlights(results)

        # 7. Update session memory
        mem = self.session_memory.get(session_id, [])
        self.session_memory[session_id] = (mem + [{"q": question, "n": len(results)}])[-10:]

        return {
            "answer": answer,
            "sql": sql,
            "results": results[:100],
            "highlighted_nodes": highlights,
            "status": "success"
        }
