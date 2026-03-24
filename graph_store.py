"""
GraphStore: Hybrid SQLite-based graph storage.

WHY HYBRID (SQLite + adjacency table) instead of pure Neo4j?
- Neo4j free tier has memory limits and requires Docker
- SQLite is zero-dependency, file-based, deployable anywhere
- Adjacency table (edges) gives us graph traversal via recursive CTEs
- Relational tables give us fast attribute lookups
- For 100K nodes this is faster than Neo4j on a single machine

TRADE-OFF: No native graph algorithms (PageRank, shortest path built-in)
SOLUTION: Implement traversal via recursive SQL CTEs (shown below)
"""
import sqlite3
import json
from typing import Optional


SCHEMA = """
-- Core entity tables (relational layer)
CREATE TABLE IF NOT EXISTS customers (
    customer_id TEXT PRIMARY KEY,
    name TEXT,
    email TEXT,
    segment TEXT,
    country TEXT,
    created_at TEXT
);

CREATE TABLE IF NOT EXISTS addresses (
    address_id TEXT PRIMARY KEY,
    customer_id TEXT,
    street TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    pincode TEXT,
    address_type TEXT  -- billing / shipping
);

CREATE TABLE IF NOT EXISTS products (
    product_id TEXT PRIMARY KEY,
    name TEXT,
    category TEXT,
    unit_price REAL,
    currency TEXT
);

CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT,
    order_date TEXT,
    status TEXT,  -- OPEN, CONFIRMED, DELIVERED, CANCELLED
    total_amount REAL,
    currency TEXT,
    sales_org TEXT,
    payment_terms TEXT
);

CREATE TABLE IF NOT EXISTS order_items (
    order_item_id TEXT PRIMARY KEY,
    order_id TEXT,
    product_id TEXT,
    quantity REAL,
    unit_price REAL,
    net_value REAL
);

CREATE TABLE IF NOT EXISTS deliveries (
    delivery_id TEXT PRIMARY KEY,
    order_id TEXT,
    ship_date TEXT,
    actual_delivery_date TEXT,
    status TEXT,  -- PLANNED, SHIPPED, DELIVERED, FAILED
    shipping_address_id TEXT,
    carrier TEXT,
    tracking_number TEXT
);

CREATE TABLE IF NOT EXISTS billing_documents (
    billing_id TEXT PRIMARY KEY,
    order_id TEXT,
    delivery_id TEXT,
    customer_id TEXT,
    invoice_date TEXT,
    due_date TEXT,
    amount REAL,
    currency TEXT,
    status TEXT,  -- OPEN, PAID, OVERDUE, CANCELLED
    billing_type TEXT  -- RV, F2, etc.
);

CREATE TABLE IF NOT EXISTS payments (
    payment_id TEXT PRIMARY KEY,
    billing_id TEXT,
    customer_id TEXT,
    payment_date TEXT,
    amount REAL,
    currency TEXT,
    method TEXT,  -- BANK_TRANSFER, CREDIT_CARD, etc.
    reference TEXT
);

CREATE TABLE IF NOT EXISTS journal_entries (
    journal_id TEXT PRIMARY KEY,
    billing_id TEXT,
    gl_account TEXT,
    amount REAL,
    currency TEXT,
    posting_date TEXT,
    document_type TEXT,
    fiscal_year TEXT,
    company_code TEXT
);

-- GRAPH LAYER: Adjacency table for all relationships
-- This is what enables graph traversal without Neo4j
CREATE TABLE IF NOT EXISTS edges (
    edge_id INTEGER PRIMARY KEY AUTOINCREMENT,
    from_type TEXT NOT NULL,   -- e.g. 'order'
    from_id TEXT NOT NULL,
    relationship TEXT NOT NULL, -- e.g. 'FULFILLED_BY'
    to_type TEXT NOT NULL,     -- e.g. 'delivery'
    to_id TEXT NOT NULL,
    properties TEXT            -- JSON for edge metadata
);

CREATE INDEX IF NOT EXISTS idx_edges_from ON edges(from_type, from_id);
CREATE INDEX IF NOT EXISTS idx_edges_to ON edges(to_type, to_id);
CREATE INDEX IF NOT EXISTS idx_edges_rel ON edges(relationship);
"""


class GraphStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")  # concurrent reads
        self._init_schema()

    def _init_schema(self):
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def execute(self, sql: str, params=()) -> list:
        """Safe query execution returning list of dicts"""
        try:
            cur = self.conn.execute(sql, params)
            rows = cur.fetchall()
            return [dict(r) for r in rows]
        except Exception as e:
            raise RuntimeError(f"Query failed: {sql[:100]}... | Error: {e}")

    def execute_write(self, sql: str, params=()):
        self.conn.execute(sql, params)
        self.conn.commit()

    def executemany_write(self, sql: str, params_list: list):
        self.conn.executemany(sql, params_list)
        self.conn.commit()

    def add_edge(self, from_type: str, from_id: str, rel: str,
                 to_type: str, to_id: str, properties: dict = None):
        props = json.dumps(properties) if properties else None
        self.execute_write(
            "INSERT OR IGNORE INTO edges (from_type, from_id, relationship, to_type, to_id, properties) VALUES (?,?,?,?,?,?)",
            (from_type, from_id, rel, to_type, to_id, props)
        )

    def get_neighbors(self, node_type: str, node_id: str, direction: str = "both") -> list:
        """Get all neighboring nodes (1-hop)"""
        if direction == "out":
            rows = self.execute(
                "SELECT * FROM edges WHERE from_type=? AND from_id=?",
                (node_type, node_id)
            )
        elif direction == "in":
            rows = self.execute(
                "SELECT * FROM edges WHERE to_type=? AND to_id=?",
                (node_type, node_id)
            )
        else:
            out = self.execute("SELECT * FROM edges WHERE from_type=? AND from_id=?", (node_type, node_id))
            inp = self.execute("SELECT * FROM edges WHERE to_type=? AND to_id=?", (node_type, node_id))
            rows = out + inp
        return rows

    def traverse(self, start_type: str, start_id: str, max_depth: int = 3) -> list:
        """
        Multi-hop traversal using recursive CTE.
        This is the key graph capability - no Neo4j needed.
        """
        sql = """
        WITH RECURSIVE traverse(from_type, from_id, relationship, to_type, to_id, depth, path) AS (
            -- Base case: direct neighbors
            SELECT from_type, from_id, relationship, to_type, to_id, 1,
                   from_type || ':' || from_id || '->' || relationship || '->' || to_type || ':' || to_id
            FROM edges
            WHERE from_type = ? AND from_id = ?
            
            UNION ALL
            
            -- Recursive case: expand outward
            SELECT e.from_type, e.from_id, e.relationship, e.to_type, e.to_id,
                   t.depth + 1,
                   t.path || '->' || e.relationship || '->' || e.to_type || ':' || e.to_id
            FROM edges e
            JOIN traverse t ON e.from_type = t.to_type AND e.from_id = t.to_id
            WHERE t.depth < ?
              AND t.path NOT LIKE '%' || e.to_type || ':' || e.to_id || '%'  -- cycle prevention
        )
        SELECT DISTINCT from_type, from_id, relationship, to_type, to_id, depth
        FROM traverse
        ORDER BY depth
        """
        return self.execute(sql, (start_type, start_id, max_depth))

    def get_schema_summary(self) -> str:
        """Returns schema info for LLM context injection"""
        tables = self.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        schema_parts = []
        for t in tables:
            name = t['name']
            if name == 'edges':
                continue
            cols = self.execute(f"PRAGMA table_info({name})")
            col_names = [c['name'] for c in cols]
            schema_parts.append(f"  {name}({', '.join(col_names)})")
        return "\n".join(schema_parts)

    def node_count(self) -> dict:
        tables = ['customers', 'orders', 'deliveries', 'billing_documents', 'payments', 'products']
        counts = {}
        for t in tables:
            r = self.execute(f"SELECT COUNT(*) as c FROM {t}")
            counts[t] = r[0]['c']
        return counts

    def get_node_attributes(self, node_type: str, node_id: str) -> Optional[dict]:
        """Fetch node attributes from the correct table"""
        table_map = {
            'customer': ('customers', 'customer_id'),
            'order': ('orders', 'order_id'),
            'delivery': ('deliveries', 'delivery_id'),
            'billing': ('billing_documents', 'billing_id'),
            'payment': ('payments', 'payment_id'),
            'product': ('products', 'product_id'),
            'address': ('addresses', 'address_id'),
            'journal': ('journal_entries', 'journal_id'),
        }
        if node_type not in table_map:
            return None
        table, pk = table_map[node_type]
        rows = self.execute(f"SELECT * FROM {table} WHERE {pk} = ?", (node_id,))
        return rows[0] if rows else None
