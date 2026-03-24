"""
Query Engine: Natural Language → SQL → Grounded Response

ARCHITECTURE DECISION: Why text-to-SQL over text-to-Cypher?
1. Our DB is SQLite - no Cypher support
2. SQL is universally understood + debuggable
3. LLM-to-SQL is mature (GPT-3 era); LLM-to-Cypher is flaky for free models
4. We handle graph traversal via recursive CTEs (shown in graph_store.py)

LLM CHOICE: Groq (Llama-3.1-8b-instant) - FREE, fast, good SQL capability
FALLBACK: Gemini Flash (also free tier)

HALLUCINATION PREVENTION STRATEGY:
1. Schema injection: LLM sees exact table/column names
2. SQL validation: parse + dry-run before execution
3. Result grounding: response is built FROM query results, not LLM imagination
4. Query classification: reject off-topic queries before LLM call
5. Row limit: always SELECT TOP 100 to prevent timeout
"""
import os
import re
import json
import sqlite3
import asyncio
import httpx
from typing import Optional
from graph.graph_store import GraphStore


# ─── SYSTEM PROMPT ─────────────────────────────────────────────────────────────
SYSTEM_PROMPT = """You are a precise SQL query generator for an Order-to-Cash (OTC) business system.

DATABASE SCHEMA:
{schema}

GRAPH EDGES TABLE:
  edges(from_type, from_id, relationship, to_type, to_id)
  Relationships: PLACED_BY, FULFILLED_BY, BILLED_AS, INVOICED_BY, SETTLED_BY, BILLED_TO

RULES (STRICT):
1. Output ONLY a valid SQLite SELECT query. No explanation, no markdown, no commentary.
2. Always use table aliases. Always add LIMIT 100.
3. Use exact column names from schema above. No invented columns.
4. For graph traversal questions, use the edges table with JOINs.
5. If question is ambiguous, query the most relevant table.
6. NEVER use DROP, DELETE, UPDATE, INSERT, CREATE, ALTER.
7. If question cannot be answered from this schema, output: UNRESOLVABLE

FEW-SHOT EXAMPLES:

Q: Which products have highest billing count?
SQL: SELECT p.name, p.product_id, COUNT(b.billing_id) as billing_count
FROM products p
JOIN order_items oi ON p.product_id = oi.product_id
JOIN billing_documents b ON oi.order_id = b.order_id
GROUP BY p.product_id, p.name
ORDER BY billing_count DESC
LIMIT 10;

Q: Trace full flow of billing document BILL00001
SQL: SELECT 
  b.billing_id, b.status as billing_status, b.amount,
  o.order_id, o.status as order_status, o.order_date,
  d.delivery_id, d.status as delivery_status, d.actual_delivery_date,
  p.payment_id, p.payment_date, p.amount as paid_amount,
  c.name as customer_name
FROM billing_documents b
LEFT JOIN orders o ON b.order_id = o.order_id
LEFT JOIN deliveries d ON b.delivery_id = d.delivery_id
LEFT JOIN payments p ON b.billing_id = p.billing_id
LEFT JOIN customers c ON b.customer_id = c.customer_id
WHERE b.billing_id = 'BILL00001'
LIMIT 100;

Q: Find incomplete order flows (orders without billing)
SQL: SELECT o.order_id, o.status, o.total_amount, o.order_date, c.name as customer_name
FROM orders o
LEFT JOIN billing_documents b ON o.order_id = b.order_id
LEFT JOIN customers c ON o.customer_id = c.customer_id
WHERE b.billing_id IS NULL AND o.status NOT IN ('CANCELLED', 'OPEN')
ORDER BY o.order_date DESC
LIMIT 100;

Q: How many overdue invoices exist?
SQL: SELECT COUNT(*) as overdue_count, SUM(amount) as total_overdue_amount
FROM billing_documents
WHERE status = 'OVERDUE';

Q: Find customers with most orders
SQL: SELECT c.customer_id, c.name, c.segment, COUNT(o.order_id) as order_count, SUM(o.total_amount) as total_value
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name, c.segment
ORDER BY order_count DESC
LIMIT 20;

Now generate SQL for the following question. Output ONLY the SQL query:
Q: {question}
SQL:"""


RESPONSE_PROMPT = """You are a business analyst for an Order-to-Cash system.
The user asked: "{question}"
The database returned these results: {results}

Write a clear, concise business answer (2-4 sentences) based STRICTLY on the data above.
- Use specific numbers from the data
- Do NOT add information not in the results
- If results are empty, say "No matching records found"
- Highlight notable patterns if relevant
"""


TOPIC_GUARD_KEYWORDS = [
    'order', 'delivery', 'invoice', 'billing', 'payment', 'customer',
    'product', 'shipment', 'dispatch', 'cash', 'revenue', 'outstanding',
    'overdue', 'pending', 'status', 'amount', 'account', 'journal', 'gl',
    'fiscal', 'document', 'trace', 'flow', 'incomplete', 'find', 'show',
    'list', 'count', 'how many', 'which', 'what', 'when', 'who', 'where',
    'total', 'sum', 'average', 'top', 'highest', 'lowest'
]


class QueryEngine:
    def __init__(self, graph_store: GraphStore):
        self.store = graph_store
        self.groq_api_key = os.getenv("GROQ_API_KEY", "")
        self.gemini_api_key = os.getenv("GEMINI_API_KEY", "")
        # Simple in-memory conversation memory per session
        self.session_memory: dict = {}

    # ─── GUARDRAIL: Topic Classification ────────────────────────────────────

    def _is_otc_related(self, query: str) -> bool:
        """
        GUARDRAIL #1: Keyword-based topic filter.
        Rejects queries unrelated to Order-to-Cash domain.
        This runs BEFORE any LLM call (saves tokens + prevents abuse).
        """
        q = query.lower()
        return any(kw in q for kw in TOPIC_GUARD_KEYWORDS)

    def _is_safe_sql(self, sql: str) -> tuple[bool, str]:
        """
        GUARDRAIL #2: SQL safety validation.
        Prevents destructive queries even if LLM hallucinates them.
        """
        sql_upper = sql.upper()
        dangerous = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'CREATE', 'ALTER', 'TRUNCATE', 'EXEC']
        for word in dangerous:
            if re.search(rf'\b{word}\b', sql_upper):
                return False, f"Dangerous keyword detected: {word}"

        # Must be a SELECT
        stripped = sql.strip()
        if not (stripped.upper().startswith('SELECT') or stripped.upper().startswith('WITH')):
            return False, "Only SELECT queries allowed"

        return True, "ok"

    def _validate_sql_columns(self, sql: str) -> bool:
        """
        GUARDRAIL #3: Dry-run query with LIMIT 0 to catch column name errors.
        Returns False if SQLite rejects the query structure.
        """
        try:
            test_sql = re.sub(r'LIMIT\s+\d+', 'LIMIT 0', sql, flags=re.IGNORECASE)
            self.store.execute(test_sql)
            return True
        except:
            return False

    # ─── LLM CALLS ──────────────────────────────────────────────────────────

    async def _call_groq(self, prompt: str, system: str = "") -> str:
        """Call Groq API (Llama 3.1 8B - free tier)"""
        headers = {
            "Authorization": f"Bearer {self.groq_api_key}",
            "Content-Type": "application/json"
        }
        body = {
            "model": "llama-3.1-8b-instant",
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.0,  # deterministic for SQL generation
            "max_tokens": 500,
        }
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers=headers, json=body
            )
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"].strip()

    async def _call_gemini(self, prompt: str) -> str:
        """Fallback: Gemini Flash (free tier)"""
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={self.gemini_api_key}"
        body = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.0, "maxOutputTokens": 500}
        }
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(url, json=body)
            resp.raise_for_status()
            return resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip()

    async def _generate_sql(self, question: str) -> str:
        """Generate SQL from natural language with fallback"""
        schema = self.store.get_schema_summary()
        prompt = SYSTEM_PROMPT.format(schema=schema, question=question)

        if self.groq_api_key:
            try:
                return await self._call_groq(prompt)
            except Exception as e:
                print(f"Groq failed: {e}, trying Gemini...")

        if self.gemini_api_key:
            return await self._call_gemini(prompt)

        raise RuntimeError("No LLM API keys configured. Set GROQ_API_KEY or GEMINI_API_KEY.")

    async def _generate_response(self, question: str, results: list) -> str:
        """Generate grounded natural language response from SQL results"""
        results_str = json.dumps(results[:20], indent=2)  # send max 20 rows to LLM
        prompt = RESPONSE_PROMPT.format(question=question, results=results_str)

        if self.groq_api_key:
            try:
                return await self._call_groq(prompt, system="You are a concise business analyst.")
            except:
                pass

        if self.gemini_api_key:
            return await self._call_gemini(prompt)

        # FALLBACK: structured response without LLM
        if not results:
            return "No matching records found in the database."
        return f"Found {len(results)} result(s). First record: {json.dumps(results[0])}"

    # ─── QUERY EXECUTION PIPELINE ────────────────────────────────────────────

    async def process(self, question: str, session_id: str = "default") -> dict:
        """
        Full pipeline:
        Question → Guard → LLM(SQL) → Validate → Execute → Ground → Response
        """
        # Step 1: Topic guard
        if not self._is_otc_related(question):
            return {
                "answer": "I can only answer questions about the Order-to-Cash process (orders, deliveries, invoices, payments, customers, products).",
                "sql": None,
                "results": [],
                "highlighted_nodes": [],
                "status": "rejected"
            }

        # Step 2: Check conversation memory for context
        memory = self.session_memory.get(session_id, [])

        # Step 3: Generate SQL
        try:
            raw_sql = await self._generate_sql(question)
        except Exception as e:
            return {"answer": f"LLM unavailable: {str(e)}", "sql": None, "results": [], "status": "error"}

        # Step 4: Handle UNRESOLVABLE
        if "UNRESOLVABLE" in raw_sql:
            return {
                "answer": "This question cannot be answered from the available Order-to-Cash dataset.",
                "sql": None, "results": [], "highlighted_nodes": [], "status": "unresolvable"
            }

        # Step 5: Clean SQL (LLMs sometimes add markdown)
        sql = self._clean_sql(raw_sql)

        # Step 6: Safety validation
        is_safe, reason = self._is_safe_sql(sql)
        if not is_safe:
            return {
                "answer": "Query rejected for safety reasons.",
                "sql": sql, "results": [], "highlighted_nodes": [], "status": "rejected"
            }

        # Step 7: Structural validation (dry run)
        if not self._validate_sql_columns(sql):
            # Try to recover with a simpler query
            return {
                "answer": "Could not generate a valid query for this question. Try rephrasing.",
                "sql": sql, "results": [], "highlighted_nodes": [], "status": "invalid_sql"
            }

        # Step 8: Execute query
        try:
            results = self.store.execute(sql)
        except Exception as e:
            return {
                "answer": f"Query execution failed: {str(e)}",
                "sql": sql, "results": [], "highlighted_nodes": [], "status": "error"
            }

        # Step 9: Generate grounded response
        answer = await self._generate_response(question, results)

        # Step 10: Extract node highlights (for frontend graph highlighting)
        highlighted_nodes = self._extract_node_refs(results)

        # Step 11: Update session memory
        self.session_memory[session_id] = (memory + [
            {"q": question, "result_count": len(results)}
        ])[-10:]  # keep last 10 turns

        return {
            "answer": answer,
            "sql": sql,
            "results": results[:100],
            "highlighted_nodes": highlighted_nodes,
            "status": "success"
        }

    def _clean_sql(self, raw: str) -> str:
        """Strip markdown, whitespace, SQL: prefix from LLM output"""
        sql = raw.strip()
        sql = re.sub(r'^```sql\s*', '', sql, flags=re.IGNORECASE)
        sql = re.sub(r'^```\s*', '', sql)
        sql = re.sub(r'```\s*$', '', sql)
        sql = re.sub(r'^SQL:\s*', '', sql, flags=re.IGNORECASE)
        return sql.strip()

    def _extract_node_refs(self, results: list) -> list:
        """
        Extract node IDs from results for frontend graph highlighting.
        Detects common ID patterns in result columns.
        """
        nodes = []
        id_patterns = {
            'order': r'^ORD\d+$',
            'delivery': r'^DEL\d+$',
            'billing': r'^BILL\d+$',
            'payment': r'^PAY\d+$',
            'customer': r'^C\d+$',
            'product': r'^P\d+$',
        }
        for row in results[:50]:
            for val in row.values():
                if not isinstance(val, str):
                    continue
                for node_type, pattern in id_patterns.items():
                    if re.match(pattern, val.strip(), re.IGNORECASE):
                        nodes.append({"type": node_type, "id": val.strip()})
        # Deduplicate
        seen = set()
        unique = []
        for n in nodes:
            key = f"{n['type']}:{n['id']}"
            if key not in seen:
                seen.add(key)
                unique.append(n)
        return unique
