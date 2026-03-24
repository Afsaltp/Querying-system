"""
Graph API: Node operations, expansion, broken flow detection

BROKEN FLOW DETECTION LOGIC:
In a healthy OTC flow: Order → Delivery → Billing → Payment
Broken = any node in the chain that has no downstream edge
We classify by severity:
  - CRITICAL: Delivered order with no billing (revenue leak)
  - WARNING: Billed with no payment + overdue
  - INFO: Open order with no delivery (might be new)
"""
from graph.graph_store import GraphStore
from typing import Optional


# Map frontend node type → color for visualization
NODE_COLORS = {
    'order': '#3B82F6',      # blue
    'delivery': '#10B981',   # green
    'billing': '#F59E0B',    # amber
    'payment': '#8B5CF6',    # purple
    'customer': '#EF4444',   # red
    'product': '#06B6D4',    # cyan
    'address': '#6B7280',    # gray
    'journal': '#F97316',    # orange
}

NODE_ICONS = {
    'order': '📦', 'delivery': '🚚', 'billing': '🧾',
    'payment': '💳', 'customer': '👤', 'product': '🏷️',
}


class GraphAPI:
    def __init__(self, store: GraphStore):
        self.store = store

    def get_node(self, node_id: str, node_type: str) -> Optional[dict]:
        """Get node attributes + its direct edges"""
        attrs = self.store.get_node_attributes(node_type, node_id)
        if not attrs:
            return None

        neighbors = self.store.get_neighbors(node_type, node_id)

        return {
            "id": node_id,
            "type": node_type,
            "color": NODE_COLORS.get(node_type, '#gray'),
            "icon": NODE_ICONS.get(node_type, '●'),
            "attributes": attrs,
            "edges": neighbors,
            "edge_count": len(neighbors)
        }

    def expand_node(self, node_id: str, node_type: str, depth: int = 1) -> dict:
        """
        Return a subgraph rooted at node_id up to `depth` hops.
        Used for interactive 'expand' in the UI.
        """
        if depth > 4:
            depth = 4  # cap to prevent runaway traversal

        edges = self.store.traverse(node_type, node_id, depth)

        # Collect unique node IDs to fetch attributes
        node_refs = {f"{node_type}:{node_id}"}
        for e in edges:
            node_refs.add(f"{e['from_type']}:{e['from_id']}")
            node_refs.add(f"{e['to_type']}:{e['to_id']}")

        nodes = []
        for ref in node_refs:
            ntype, nid = ref.split(":", 1)
            attrs = self.store.get_node_attributes(ntype, nid)
            nodes.append({
                "id": nid,
                "type": ntype,
                "color": NODE_COLORS.get(ntype, '#94a3b8'),
                "icon": NODE_ICONS.get(ntype, '●'),
                "label": self._make_label(ntype, nid, attrs),
                "attributes": attrs or {},
            })

        edge_list = [
            {
                "source": e['from_id'],
                "target": e['to_id'],
                "relationship": e['relationship'],
                "depth": e['depth']
            }
            for e in edges
        ]

        return {
            "root": {"id": node_id, "type": node_type},
            "nodes": nodes,
            "edges": edge_list,
            "depth": depth
        }

    def get_full_graph(self) -> dict:
        """
        Return the full graph for initial visualization.
        For large graphs: returns aggregated view (node type clusters).
        Threshold: if > 500 nodes, return summary clusters instead.
        """
        counts = self.store.node_count()
        total = sum(counts.values())

        if total > 500:
            return self._get_clustered_graph(counts)

        return self._get_detailed_graph()

    def _get_detailed_graph(self) -> dict:
        """Full node-by-node graph"""
        all_edges = self.store.execute("SELECT * FROM edges LIMIT 2000")

        # Collect unique node IDs
        node_refs = set()
        for e in all_edges:
            node_refs.add(f"{e['from_type']}:{e['from_id']}")
            node_refs.add(f"{e['to_type']}:{e['to_id']}")

        nodes = []
        for ref in node_refs:
            ntype, nid = ref.split(":", 1)
            nodes.append({
                "id": nid,
                "type": ntype,
                "color": NODE_COLORS.get(ntype, '#94a3b8'),
                "label": f"{ntype.upper()[:3]}-{nid[-4:]}",
            })

        edges = [
            {"source": e['from_id'], "target": e['to_id'],
             "relationship": e['relationship']}
            for e in all_edges
        ]
        return {"nodes": nodes, "edges": edges, "mode": "detailed"}

    def _get_clustered_graph(self, counts: dict) -> dict:
        """Aggregated cluster nodes for large graphs"""
        nodes = [
            {"id": ntype, "type": "cluster", "label": f"{ntype}\n({count})",
             "color": NODE_COLORS.get(ntype.rstrip('s'), '#94a3b8'),
             "size": min(count * 2, 80)}
            for ntype, count in counts.items()
        ]
        edges = [
            {"source": "orders", "target": "deliveries", "relationship": "FULFILLED_BY"},
            {"source": "orders", "target": "billing_documents", "relationship": "BILLED_AS"},
            {"source": "deliveries", "target": "billing_documents", "relationship": "INVOICED_BY"},
            {"source": "billing_documents", "target": "payments", "relationship": "SETTLED_BY"},
            {"source": "orders", "target": "customers", "relationship": "PLACED_BY"},
        ]
        return {"nodes": nodes, "edges": edges, "mode": "clustered"}

    def detect_broken_flows(self) -> dict:
        """
        BROKEN FLOW DETECTION:
        Scan for incomplete OTC chains and classify by severity.
        """
        critical, warnings, info = [], [], []

        # CRITICAL: Delivered orders with no billing (revenue leak)
        result = self.store.execute("""
            SELECT o.order_id, o.customer_id, o.total_amount, o.order_date
            FROM orders o
            LEFT JOIN billing_documents b ON o.order_id = b.order_id
            WHERE o.status = 'DELIVERED' AND b.billing_id IS NULL
            LIMIT 50
        """)
        for r in result:
            critical.append({
                "type": "MISSING_BILLING",
                "severity": "CRITICAL",
                "message": f"Order {r['order_id']} delivered but never billed (₹{r['total_amount']})",
                "node": {"id": r['order_id'], "type": "order"},
                "data": r
            })

        # WARNING: Overdue billings with no payment
        result = self.store.execute("""
            SELECT b.billing_id, b.customer_id, b.amount, b.due_date
            FROM billing_documents b
            LEFT JOIN payments p ON b.billing_id = p.billing_id
            WHERE b.status = 'OVERDUE' AND p.payment_id IS NULL
            LIMIT 50
        """)
        for r in result:
            warnings.append({
                "type": "UNPAID_OVERDUE",
                "severity": "WARNING",
                "message": f"Billing {r['billing_id']} overdue since {r['due_date']} (₹{r['amount']})",
                "node": {"id": r['billing_id'], "type": "billing"},
                "data": r
            })

        # INFO: Orders confirmed but no delivery
        result = self.store.execute("""
            SELECT o.order_id, o.order_date, o.status
            FROM orders o
            LEFT JOIN deliveries d ON o.order_id = d.order_id
            WHERE o.status = 'CONFIRMED' AND d.delivery_id IS NULL
            LIMIT 50
        """)
        for r in result:
            info.append({
                "type": "NO_DELIVERY",
                "severity": "INFO",
                "message": f"Order {r['order_id']} confirmed but no delivery created",
                "node": {"id": r['order_id'], "type": "order"},
                "data": r
            })

        return {
            "critical": critical,
            "warnings": warnings,
            "info": info,
            "summary": {
                "critical_count": len(critical),
                "warning_count": len(warnings),
                "info_count": len(info),
                "total_broken": len(critical) + len(warnings) + len(info)
            }
        }

    def _make_label(self, node_type: str, node_id: str, attrs: Optional[dict]) -> str:
        if not attrs:
            return node_id
        label_fields = {
            'customer': 'name', 'product': 'name',
            'order': 'status', 'delivery': 'status',
            'billing': 'status', 'payment': 'method',
        }
        field = label_fields.get(node_type)
        if field and field in attrs:
            return f"{node_id}\n[{attrs[field]}]"
        return node_id
