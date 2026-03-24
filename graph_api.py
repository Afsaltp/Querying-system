"""
Graph API helpers for generic OTC schema.
"""
from typing import Optional, Any
from graph_store import GraphStore

NODE_COLORS = {
    "order": "#4f8ef7",
    "delivery": "#00c4a0",
    "billing": "#f59e0b",
    "payment": "#10b981",
    "customer": "#ef4444",
    "product": "#a3e635",
}


class GraphAPI:
    def __init__(self, db_path: str):
        self.store = GraphStore(db_path)

    def _node_attrs(self, node_id: str, node_type: str) -> Optional[dict[str, Any]]:
        lookup = {
            "order": ("orders", "order_id"),
            "delivery": ("deliveries", "delivery_id"),
            "billing": ("billing_documents", "billing_id"),
            "payment": ("payments", "payment_id"),
            "customer": ("customers", "customer_id"),
            "product": ("products", "product_id"),
        }
        if node_type not in lookup:
            return None
        table, pk = lookup[node_type]
        rows = self.store.execute(f"SELECT * FROM {table} WHERE {pk}=? LIMIT 1", (node_id,))
        return rows[0] if rows else None

    def get_node(self, node_id: str, node_type: str) -> Optional[dict[str, Any]]:
        attrs = self._node_attrs(node_id, node_type)
        if not attrs:
            return None
        edges = self.store.execute(
            "SELECT from_type, from_id, relationship, to_type, to_id FROM edges WHERE "
            "(from_type=? AND from_id=?) OR (to_type=? AND to_id=?)",
            (node_type, node_id, node_type, node_id),
        )
        return {
            "id": node_id,
            "type": node_type,
            "color": NODE_COLORS.get(node_type, "#64748b"),
            "attributes": attrs,
            "edges": edges,
            "edge_count": len(edges),
        }

    def expand_node(self, node_id: str, node_type: str, depth: int = 2) -> dict[str, Any]:
        depth = max(1, min(int(depth), 4))
        sql = """
        WITH RECURSIVE walk(ft, fi, rel, tt, ti, d) AS (
            SELECT from_type, from_id, relationship, to_type, to_id, 1
            FROM edges WHERE from_type=? AND from_id=?
            UNION ALL
            SELECT e.from_type, e.from_id, e.relationship, e.to_type, e.to_id, walk.d + 1
            FROM edges e
            JOIN walk ON e.from_type = walk.tt AND e.from_id = walk.ti
            WHERE walk.d < ?
        )
        SELECT DISTINCT ft, fi, rel, tt, ti, d FROM walk ORDER BY d LIMIT 500
        """
        edge_rows = self.store.execute(sql, (node_type, node_id, depth))
        refs = {f"{node_type}:{node_id}"}
        for e in edge_rows:
            refs.add(f"{e['ft']}:{e['fi']}")
            refs.add(f"{e['tt']}:{e['ti']}")

        nodes = []
        for ref in refs:
            ntype, nid = ref.split(":", 1)
            nodes.append(
                {
                    "id": nid,
                    "type": ntype,
                    "color": NODE_COLORS.get(ntype, "#64748b"),
                    "attributes": self._node_attrs(nid, ntype) or {},
                }
            )

        edges = [
            {"source": e["fi"], "target": e["ti"], "relationship": e["rel"], "depth": e["d"]}
            for e in edge_rows
        ]
        return {"root": {"id": node_id, "type": node_type}, "nodes": nodes, "edges": edges, "depth": depth}

    def get_full_graph(self) -> dict[str, Any]:
        edge_rows = self.store.execute(
            "SELECT from_type, from_id, relationship, to_type, to_id FROM edges LIMIT 2000"
        )
        refs = set()
        for e in edge_rows:
            refs.add((e["from_type"], e["from_id"]))
            refs.add((e["to_type"], e["to_id"]))
        nodes = [{"id": nid, "type": ntype, "color": NODE_COLORS.get(ntype, "#64748b")} for ntype, nid in refs]
        edges = [
            {"source": e["from_id"], "target": e["to_id"], "relationship": e["relationship"]} for e in edge_rows
        ]
        return {"nodes": nodes, "edges": edges, "mode": "detailed"}

    def detect_broken_flows(self) -> dict[str, Any]:
        critical = self.store.execute(
            """
            SELECT o.order_id, o.status, o.total_amount, o.currency
            FROM orders o
            LEFT JOIN billing_documents b ON o.order_id = b.order_id
            WHERE b.billing_id IS NULL AND UPPER(COALESCE(o.status, '')) NOT IN ('CANCELLED', 'OPEN')
            LIMIT 50
            """
        )
        warnings = self.store.execute(
            """
            SELECT b.billing_id, b.amount, b.currency, b.status
            FROM billing_documents b
            LEFT JOIN payments p ON b.billing_id = p.billing_id
            WHERE p.payment_id IS NULL AND UPPER(COALESCE(b.status, '')) IN ('OVERDUE', 'OPEN')
            LIMIT 50
            """
        )
        return {
            "critical": [
                {
                    "type": "DELIVERED_NOT_BILLED",
                    "severity": "CRITICAL",
                    "message": f"Order {r['order_id']} has no billing document.",
                    "node": {"id": r["order_id"], "type": "order"},
                    "data": r,
                }
                for r in critical
            ],
            "warnings": [
                {
                    "type": "BILLING_NOT_SETTLED",
                    "severity": "WARNING",
                    "message": f"Billing {r['billing_id']} has no payment record.",
                    "node": {"id": r["billing_id"], "type": "billing"},
                    "data": r,
                }
                for r in warnings
            ],
            "info": [],
            "summary": {
                "critical_count": len(critical),
                "warning_count": len(warnings),
                "info_count": 0,
                "total_broken": len(critical) + len(warnings),
            },
        }
