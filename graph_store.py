"""
SQLite-backed hybrid graph store for OTC data.
"""
import sqlite3
from typing import Any


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS customers (
    customer_id TEXT PRIMARY KEY,
    name TEXT,
    email TEXT,
    segment TEXT,
    country TEXT,
    created_at TEXT
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
    status TEXT,
    total_amount REAL,
    currency TEXT,
    sales_org TEXT,
    payment_terms TEXT
);

CREATE TABLE IF NOT EXISTS deliveries (
    delivery_id TEXT PRIMARY KEY,
    order_id TEXT,
    ship_date TEXT,
    actual_delivery_date TEXT,
    status TEXT,
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
    status TEXT,
    billing_type TEXT
);

CREATE TABLE IF NOT EXISTS payments (
    payment_id TEXT PRIMARY KEY,
    billing_id TEXT,
    customer_id TEXT,
    payment_date TEXT,
    amount REAL,
    currency TEXT,
    method TEXT,
    reference TEXT
);

CREATE TABLE IF NOT EXISTS edges (
    edge_id INTEGER PRIMARY KEY AUTOINCREMENT,
    from_type TEXT NOT NULL,
    from_id TEXT NOT NULL,
    relationship TEXT NOT NULL,
    to_type TEXT NOT NULL,
    to_id TEXT NOT NULL,
    UNIQUE(from_type, from_id, relationship, to_type, to_id)
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
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.executescript(SCHEMA_SQL)
        self.conn.commit()

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        rows = self.conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def executemany_write(self, sql: str, rows: list[tuple[Any, ...]]) -> int:
        if not rows:
            return 0
        self.conn.executemany(sql, rows)
        self.conn.commit()
        return len(rows)

    def add_edge(self, from_type: str, from_id: str, relationship: str, to_type: str, to_id: str) -> None:
        if not from_id or not to_id:
            return
        self.conn.execute(
            """
            INSERT OR IGNORE INTO edges (from_type, from_id, relationship, to_type, to_id)
            VALUES (?, ?, ?, ?, ?)
            """,
            (str(from_type), str(from_id), str(relationship), str(to_type), str(to_id)),
        )
        self.conn.commit()

    def get_schema_summary(self) -> str:
        out: list[str] = []
        tables = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name != 'sqlite_sequence' ORDER BY name"
        ).fetchall()
        for (tname,) in tables:
            cols = self.conn.execute(f"PRAGMA table_info({tname})").fetchall()
            out.append(f"{tname}({', '.join(c[1] for c in cols)})")
        return "\n".join(out)
