"""
SAP Graph API — node lookup, expand, broken flow detection (flat import version)
"""
import sqlite3
from typing import Optional

NC = {
    'sales_order':      '#4f8ef7',
    'billing_document': '#f0a500',
    'payment':          '#9b72f8',
    'journal_entry':    '#06c8e8',
    'business_partner': '#ef4444',
    'delivery':         '#00c4a0',
    'material':         '#a3e635',
    'plant':            '#64748b',
}
NI = {
    'sales_order':      '📦',
    'billing_document': '🧾',
    'payment':          '💳',
    'journal_entry':    '📒',
    'business_partner': '👤',
    'delivery':         '🚚',
    'material':         '🏷️',
    'plant':            '🏭',
}


class SAPGraphAPI:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

    def _q(self, sql: str, params: tuple = ()) -> list:
        return [dict(r) for r in self.conn.execute(sql, params).fetchall()]

    # ── NODE FETCH ────────────────────────────────────────────────────────────
    def get_node(self, node_id: str, node_type: str) -> Optional[dict]:
        attrs = self._fetch_attrs(node_id, node_type)
        if not attrs:
            return None
        edges = self._q(
            "SELECT * FROM edges WHERE (from_type=? AND from_id=?) OR (to_type=? AND to_id=?)",
            (node_type, node_id, node_type, node_id)
        )
        return {
            "id": node_id,
            "type": node_type,
            "color": NC.get(node_type, '#64748b'),
            "icon": NI.get(node_type, '●'),
            "attributes": attrs,
            "edges": edges,
            "edge_count": len(edges),
        }

    def _fetch_attrs(self, node_id: str, node_type: str) -> Optional[dict]:
        lookup = {
            'sales_order':      ("sales_order_headers",      "salesOrder"),
            'billing_document': ("billing_documents",        "billingDocument"),
            'business_partner': ("business_partners",        "businessPartner"),
            'delivery':         ("outbound_delivery_headers","deliveryDocument"),
            'material':         ("product_descriptions",     "product"),
            'plant':            ("plants",                   "plant"),
        }
        if node_type == 'payment':
            parts = node_id.split("_", 1)
            if len(parts) == 2:
                rows = self._q(
                    "SELECT * FROM payments_ar WHERE accountingDocument=? AND accountingDocumentItem=? LIMIT 1",
                    (parts[0], parts[1])
                )
            else:
                rows = self._q(
                    "SELECT * FROM payments_ar WHERE accountingDocument=? LIMIT 1",
                    (node_id,)
                )
            return rows[0] if rows else None

        if node_type == 'journal_entry':
            parts = node_id.split("_", 1)
            if len(parts) == 2:
                rows = self._q(
                    "SELECT * FROM journal_entry_items_ar WHERE accountingDocument=? AND accountingDocumentItem=? LIMIT 1",
                    (parts[0], parts[1])
                )
            else:
                rows = self._q(
                    "SELECT * FROM journal_entry_items_ar WHERE accountingDocument=? LIMIT 1",
                    (node_id,)
                )
            return rows[0] if rows else None

        if node_type not in lookup:
            return None
        table, pk = lookup[node_type]
        rows = self._q(f"SELECT * FROM {table} WHERE {pk}=? LIMIT 1", (node_id,))
        return rows[0] if rows else None

    # ── GRAPH EXPAND ──────────────────────────────────────────────────────────
    def expand_node(self, node_id: str, node_type: str, depth: int = 2) -> dict:
        depth = min(depth, 4)
        sql = """
        WITH RECURSIVE t(ft,fi,rel,tt,ti,d) AS (
          SELECT from_type, from_id, relationship, to_type, to_id, 1
          FROM edges WHERE from_type=? AND from_id=?
          UNION ALL
          SELECT e.from_type, e.from_id, e.relationship, e.to_type, e.to_id, t.d+1
          FROM edges e
          JOIN t ON e.from_type=t.tt AND e.from_id=t.ti
          WHERE t.d < ?
        )
        SELECT DISTINCT ft, fi, rel, tt, ti, d FROM t ORDER BY d LIMIT 500
        """
        edges = self._q(sql, (node_type, node_id, depth))

        refs = {f"{node_type}:{node_id}"}
        for e in edges:
            refs.add(f"{e['ft']}:{e['fi']}")
            refs.add(f"{e['tt']}:{e['ti']}")

        nodes = []
        for ref in refs:
            ntype, nid = ref.split(":", 1)
            attrs = self._fetch_attrs(nid, ntype)
            nodes.append({
                "id": nid,
                "type": ntype,
                "color": NC.get(ntype, '#64748b'),
                "icon": NI.get(ntype, '●'),
                "label": self._label(ntype, nid, attrs),
                "attributes": attrs or {},
            })

        return {
            "root": {"id": node_id, "type": node_type},
            "nodes": nodes,
            "edges": [
                {"source": e["fi"], "target": e["ti"],
                 "relationship": e["rel"], "depth": e["d"]}
                for e in edges
            ],
            "depth": depth,
        }

    # ── FULL GRAPH ────────────────────────────────────────────────────────────
    def get_full_graph(self) -> dict:
        edges = self._q(
            "SELECT from_type, from_id, relationship, to_type, to_id FROM edges LIMIT 2000"
        )
        refs = set()
        for e in edges:
            refs.add(f"{e['from_type']}:{e['from_id']}")
            refs.add(f"{e['to_type']}:{e['to_id']}")

        nodes = []
        type_counts: dict = {}
        for ref in refs:
            ntype, nid = ref.split(":", 1)
            type_counts[ntype] = type_counts.get(ntype, 0) + 1
            nodes.append({
                "id": nid,
                "type": ntype,
                "color": NC.get(ntype, '#64748b'),
                "label": nid[-8:],
            })

        return {
            "nodes": nodes,
            "edges": [
                {"source": e["from_id"], "target": e["to_id"],
                 "relationship": e["relationship"]}
                for e in edges
            ],
            "mode": "detailed",
            "summary": type_counts,
        }

    # ── BROKEN FLOW DETECTION ─────────────────────────────────────────────────
    def detect_broken_flows(self) -> dict:
        critical, warnings, info = [], [], []

        # CRITICAL: Delivery complete but no billing
        rows = self._q("""
            SELECT s.salesOrder, s.soldToParty, s.totalNetAmount,
                   s.overallDeliveryStatus, s.transactionCurrency
            FROM sales_order_headers s
            LEFT JOIN billing_documents b ON s.salesOrder = b.soldToParty
            WHERE s.overallDeliveryStatus = 'C' AND b.billingDocument IS NULL
            LIMIT 30
        """)
        for r in rows:
            critical.append({
                "type": "DELIVERED_NOT_BILLED",
                "severity": "CRITICAL",
                "message": (f"Order {r['salesOrder']} delivery complete — "
                            f"no billing document "
                            f"({r['transactionCurrency']} {r['totalNetAmount']})"),
                "node": {"id": r["salesOrder"], "type": "sales_order"},
                "data": r,
            })

        # WARNING: Cancelled billing documents
        rows = self._q("""
            SELECT billingDocument, totalNetAmount, transactionCurrency,
                   soldToParty, cancelledBillingDocument
            FROM billing_documents
            WHERE billingDocumentIsCancelled = 1
            LIMIT 30
        """)
        for r in rows:
            warnings.append({
                "type": "CANCELLED_BILLING",
                "severity": "WARNING",
                "message": (f"Billing {r['billingDocument']} was cancelled "
                            f"({r['transactionCurrency']} {r['totalNetAmount']})"),
                "node": {"id": r["billingDocument"], "type": "billing_document"},
                "data": r,
            })

        # WARNING: Billing with no journal entry
        rows = self._q("""
            SELECT b.billingDocument, b.totalNetAmount, b.transactionCurrency, b.soldToParty
            FROM billing_documents b
            LEFT JOIN journal_entry_items_ar j ON b.accountingDocument = j.accountingDocument
            WHERE j.accountingDocument IS NULL AND b.billingDocumentIsCancelled = 0
            LIMIT 20
        """)
        for r in rows:
            warnings.append({
                "type": "NO_JOURNAL_ENTRY",
                "severity": "WARNING",
                "message": (f"Billing {r['billingDocument']} has no journal entry "
                            f"({r['transactionCurrency']} {r['totalNetAmount']})"),
                "node": {"id": r["billingDocument"], "type": "billing_document"},
                "data": r,
            })

        # INFO: Journal entries with no payment clearing
        rows = self._q("""
            SELECT j.accountingDocument, j.referenceDocument,
                   j.amountInTransactionCurrency, j.transactionCurrency
            FROM journal_entry_items_ar j
            LEFT JOIN payments_ar p ON j.accountingDocument = p.accountingDocument
            WHERE p.accountingDocument IS NULL
            LIMIT 20
        """)
        for r in rows:
            info.append({
                "type": "UNCLEARED_JOURNAL",
                "severity": "INFO",
                "message": (f"Journal {r['accountingDocument']} "
                            f"(ref: {r['referenceDocument']}) has no payment "
                            f"({r['transactionCurrency']} {r['amountInTransactionCurrency']})"),
                "node": {"id": str(r["accountingDocument"]) + "_1", "type": "journal_entry"},
                "data": r,
            })

        return {
            "critical": critical,
            "warnings": warnings,
            "info": info,
            "summary": {
                "critical_count": len(critical),
                "warning_count": len(warnings),
                "info_count": len(info),
                "total_broken": len(critical) + len(warnings) + len(info),
            },
        }

    def _label(self, ntype: str, nid: str, attrs: Optional[dict]) -> str:
        if not attrs:
            return nid[-10:]
        field_map = {
            "billing_document": "billingDocumentType",
            "business_partner": "businessPartnerFullName",
            "material":         "productDescription",
        }
        field = field_map.get(ntype)
        if field and attrs.get(field):
            return f"{nid[-8:]}\n[{str(attrs[field])[:15]}]"
        return nid[-10:]
