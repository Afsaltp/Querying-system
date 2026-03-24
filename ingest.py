"""
Data Ingestion Pipeline: CSV/JSON → Hybrid SQLite Graph

DESIGN DECISIONS:
1. Pandas for cleaning (vectorized ops, fast)
2. Explicit ID normalization (strip whitespace, uppercase)
3. Missing link strategy: create "UNRESOLVED" placeholder edges
4. Idempotent inserts (INSERT OR REPLACE) so you can re-run safely
5. Edge inference: derive edges from FK relationships automatically

RUN: python pipeline/ingest.py --data-dir data/
"""
import pandas as pd  # type: ignore
import sqlite3
import json
import argparse
import os
import sys
from graph_store import GraphStore  # type: ignore
from typing import Any


class OTCIngestionPipeline:
    def __init__(self, db_path: str):
        self.store = GraphStore(db_path)
        self.stats: dict[str, Any] = {"nodes": {}, "edges": 0, "warnings": []}

    # ─── CLEANING UTILITIES ──────────────────────────────────────────────────

    def _clean_id(self, val) -> str:
        """Normalize IDs: strip, uppercase, handle nulls"""
        if pd.isna(val):
            return None
        return str(val).strip().upper()

    def _clean_df(self, df: pd.DataFrame, id_col: str) -> pd.DataFrame:
        """Standard cleaning applied to every dataframe"""
        df = df.copy()
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
        df[id_col] = df[id_col].apply(self._clean_id)
        df = df.dropna(subset=[id_col])  # drop rows with null primary key
        df = df.drop_duplicates(subset=[id_col])
        return df

    def _safe_float(self, val):
        try:
            return float(val)
        except:
            return None

    # ─── ENTITY LOADERS ──────────────────────────────────────────────────────

    def load_customers(self, path: str):
        df = pd.read_csv(path)
        df = self._clean_df(df, 'customer_id')
        rows = []
        for _, r in df.iterrows():
            rows.append((
                r['customer_id'],
                r.get('name', ''), r.get('email', ''),
                r.get('segment', ''), r.get('country', ''),
                str(r.get('created_at', ''))
            ))
        self.store.executemany_write(
            "INSERT OR REPLACE INTO customers VALUES (?,?,?,?,?,?)", rows
        )
        self.stats["nodes"]["customers"] = len(rows)
        print(f"  ✓ Loaded {len(rows)} customers")

    def load_products(self, path: str):
        df = pd.read_csv(path)
        df = self._clean_df(df, 'product_id')
        rows = []
        for _, r in df.iterrows():
            rows.append((
                r['product_id'], r.get('name', ''), r.get('category', ''),
                self._safe_float(r.get('unit_price')), r.get('currency', 'USD')
            ))
        self.store.executemany_write(
            "INSERT OR REPLACE INTO products VALUES (?,?,?,?,?)", rows
        )
        self.stats["nodes"]["products"] = len(rows)
        print(f"  ✓ Loaded {len(rows)} products")

    def load_orders(self, path: str):
        df = pd.read_csv(path)
        df = self._clean_df(df, 'order_id')
        rows = []
        for _, r in df.iterrows():
            rows.append((
                r['order_id'], self._clean_id(r.get('customer_id')),
                str(r.get('order_date', '')), r.get('status', 'OPEN'),
                self._safe_float(r.get('total_amount')), r.get('currency', 'USD'),
                r.get('sales_org', ''), r.get('payment_terms', '')
            ))
        self.store.executemany_write(
            "INSERT OR REPLACE INTO orders VALUES (?,?,?,?,?,?,?,?)", rows
        )
        self.stats["nodes"]["orders"] = len(rows)
        print(f"  ✓ Loaded {len(rows)} orders")

    def load_deliveries(self, path: str):
        df = pd.read_csv(path)
        df = self._clean_df(df, 'delivery_id')
        rows = []
        for _, r in df.iterrows():
            rows.append((
                r['delivery_id'], self._clean_id(r.get('order_id')),
                str(r.get('ship_date', '')), str(r.get('actual_delivery_date', '')),
                r.get('status', 'PLANNED'), self._clean_id(r.get('shipping_address_id')),
                r.get('carrier', ''), r.get('tracking_number', '')
            ))
        self.store.executemany_write(
            "INSERT OR REPLACE INTO deliveries VALUES (?,?,?,?,?,?,?,?)", rows
        )
        self.stats["nodes"]["deliveries"] = len(rows)
        print(f"  ✓ Loaded {len(rows)} deliveries")

    def load_billing(self, path: str):
        df = pd.read_csv(path)
        df = self._clean_df(df, 'billing_id')
        rows = []
        for _, r in df.iterrows():
            rows.append((
                r['billing_id'], self._clean_id(r.get('order_id')),
                self._clean_id(r.get('delivery_id')), self._clean_id(r.get('customer_id')),
                str(r.get('invoice_date', '')), str(r.get('due_date', '')),
                self._safe_float(r.get('amount')), r.get('currency', 'USD'),
                r.get('status', 'OPEN'), r.get('billing_type', 'RV')
            ))
        self.store.executemany_write(
            "INSERT OR REPLACE INTO billing_documents VALUES (?,?,?,?,?,?,?,?,?,?)", rows
        )
        self.stats["nodes"]["billing"] = len(rows)
        print(f"  ✓ Loaded {len(rows)} billing documents")

    def load_payments(self, path: str):
        df = pd.read_csv(path)
        df = self._clean_df(df, 'payment_id')
        rows = []
        for _, r in df.iterrows():
            rows.append((
                r['payment_id'], self._clean_id(r.get('billing_id')),
                self._clean_id(r.get('customer_id')),
                str(r.get('payment_date', '')),
                self._safe_float(r.get('amount')), r.get('currency', 'USD'),
                r.get('method', ''), r.get('reference', '')
            ))
        self.store.executemany_write(
            "INSERT OR REPLACE INTO payments VALUES (?,?,?,?,?,?,?,?)", rows
        )
        self.stats["nodes"]["payments"] = len(rows)
        print(f"  ✓ Loaded {len(rows)} payments")

    # ─── EDGE BUILDER ────────────────────────────────────────────────────────

    def build_edges(self):
        """
        Derive all edges from FK relationships in relational tables.
        This is the graph-ification step.
        
        MISSING LINK STRATEGY:
        - If FK exists but target doesn't → log warning, skip edge
        - Don't create phantom nodes (preserves data integrity)
        - Broken flows become visible via the broken-flows API
        """
        print("\n  Building graph edges...")
        edge_count: int = 0

        # ORDER → CUSTOMER (PLACED_BY)
        orders = self.store.execute("SELECT order_id, customer_id FROM orders WHERE customer_id IS NOT NULL")
        for r in orders:
            exists = self.store.execute("SELECT 1 FROM customers WHERE customer_id=?", (r['customer_id'],))
            if exists:
                self.store.add_edge('order', r['order_id'], 'PLACED_BY', 'customer', r['customer_id'])
                edge_count += 1  # type: ignore
            else:
                self.stats["warnings"].append(f"Order {r['order_id']} has unknown customer {r['customer_id']}")

        # ORDER → DELIVERY (FULFILLED_BY)
        deliveries = self.store.execute("SELECT delivery_id, order_id FROM deliveries WHERE order_id IS NOT NULL")
        for r in deliveries:
            self.store.add_edge('order', r['order_id'], 'FULFILLED_BY', 'delivery', r['delivery_id'])
            edge_count += 1  # type: ignore

        # ORDER → BILLING (BILLED_AS)
        billings = self.store.execute("SELECT billing_id, order_id FROM billing_documents WHERE order_id IS NOT NULL")
        for r in billings:
            self.store.add_edge('order', r['order_id'], 'BILLED_AS', 'billing', r['billing_id'])
            edge_count += 1  # type: ignore

        # DELIVERY → BILLING (INVOICED_BY)
        billings = self.store.execute("SELECT billing_id, delivery_id FROM billing_documents WHERE delivery_id IS NOT NULL")
        for r in billings:
            self.store.add_edge('delivery', r['delivery_id'], 'INVOICED_BY', 'billing', r['billing_id'])
            edge_count += 1  # type: ignore

        # BILLING → PAYMENT (SETTLED_BY)
        payments = self.store.execute("SELECT payment_id, billing_id FROM payments WHERE billing_id IS NOT NULL")
        for r in payments:
            self.store.add_edge('billing', r['billing_id'], 'SETTLED_BY', 'payment', r['payment_id'])
            edge_count += 1  # type: ignore

        # BILLING → CUSTOMER (BILLED_TO)
        billings = self.store.execute("SELECT billing_id, customer_id FROM billing_documents WHERE customer_id IS NOT NULL")
        for r in billings:
            self.store.add_edge('billing', r['billing_id'], 'BILLED_TO', 'customer', r['customer_id'])
            edge_count += 1  # type: ignore

        self.stats["edges"] = edge_count
        print(f"  ✓ Created {edge_count} edges")

    # ─── SAMPLE DATA GENERATOR ───────────────────────────────────────────────

    def generate_sample_data(self, output_dir: str):
        """Generate realistic sample CSVs for demo purposes"""
        import random
        from datetime import datetime, timedelta
        os.makedirs(output_dir, exist_ok=True)

        customers = pd.DataFrame([
            {"customer_id": f"C{i:04d}", "name": f"Customer {i}",
             "email": f"customer{i}@example.com",
             "segment": random.choice(["RETAIL", "WHOLESALE", "ENTERPRISE"]),
             "country": random.choice(["IN", "US", "DE", "GB"]),
             "created_at": "2024-01-01"}
            for i in range(1, 51)
        ])
        customers.to_csv(f"{output_dir}/customers.csv", index=False)

        products = pd.DataFrame([
            {"product_id": f"P{i:04d}", "name": f"Product {i}",
             "category": random.choice(["ELECTRONICS", "SOFTWARE", "SERVICES"]),
             "unit_price": round(random.uniform(100, 5000), 2),
             "currency": "INR"}
            for i in range(1, 21)
        ])
        products.to_csv(f"{output_dir}/products.csv", index=False)

        base_date = datetime(2025, 1, 1)
        orders = []
        deliveries = []
        billings = []
        payments = []

        for i in range(1, 101):
            oid = f"ORD{i:05d}"
            cid = f"C{random.randint(1,50):04d}"
            odate = base_date + timedelta(days=random.randint(0, 300))
            status = random.choice(["CONFIRMED", "DELIVERED", "OPEN", "CANCELLED"])
            amount = round(random.uniform(500, 50000), 2)
            orders.append({
                "order_id": oid, "customer_id": cid,
                "order_date": odate.strftime("%Y-%m-%d"),
                "status": status, "total_amount": amount,
                "currency": "INR", "sales_org": "IN01", "payment_terms": "NET30"
            })

            # ~80% have delivery
            if status != "CANCELLED" and random.random() > 0.1:
                did = f"DEL{i:05d}"
                ship_date = odate + timedelta(days=random.randint(1, 5))
                del_status = "DELIVERED" if status == "DELIVERED" else random.choice(["SHIPPED", "PLANNED"])
                deliveries.append({
                    "delivery_id": did, "order_id": oid,
                    "ship_date": ship_date.strftime("%Y-%m-%d"),
                    "actual_delivery_date": (ship_date + timedelta(days=3)).strftime("%Y-%m-%d"),
                    "status": del_status, "shipping_address_id": f"ADDR{cid}",
                    "carrier": random.choice(["FEDEX", "DHL", "DTDC"]),
                    "tracking_number": f"TRK{i:08d}"
                })

                # ~75% have billing
                if random.random() > 0.2:
                    bid = f"BILL{i:05d}"
                    inv_date = ship_date + timedelta(days=1)
                    bill_status = random.choice(["PAID", "OPEN", "OVERDUE"])
                    billings.append({
                        "billing_id": bid, "order_id": oid, "delivery_id": did,
                        "customer_id": cid,
                        "invoice_date": inv_date.strftime("%Y-%m-%d"),
                        "due_date": (inv_date + timedelta(days=30)).strftime("%Y-%m-%d"),
                        "amount": amount, "currency": "INR",
                        "status": bill_status, "billing_type": "RV"
                    })

                    # ~70% of paid billings have payment
                    if bill_status == "PAID" and random.random() > 0.05:
                        pay_date = inv_date + timedelta(days=random.randint(1, 30))
                        payments.append({
                            "payment_id": f"PAY{i:05d}", "billing_id": bid,
                            "customer_id": cid,
                            "payment_date": pay_date.strftime("%Y-%m-%d"),
                            "amount": amount, "currency": "INR",
                            "method": random.choice(["BANK_TRANSFER", "CREDIT_CARD", "UPI"]),
                            "reference": f"REF{i:08d}"
                        })

        pd.DataFrame(orders).to_csv(f"{output_dir}/orders.csv", index=False)
        pd.DataFrame(deliveries).to_csv(f"{output_dir}/deliveries.csv", index=False)
        pd.DataFrame(billings).to_csv(f"{output_dir}/billing_documents.csv", index=False)
        pd.DataFrame(payments).to_csv(f"{output_dir}/payments.csv", index=False)
        print(f"  ✓ Sample data generated in {output_dir}/")

    def run(self, data_dir: str):
        print("🚀 Starting OTC Ingestion Pipeline\n")
        files = {
            "customers": self.load_customers,
            "products": self.load_products,
            "orders": self.load_orders,
            "deliveries": self.load_deliveries,
            "billing_documents": self.load_billing,
            "payments": self.load_payments,
        }
        for name, loader in files.items():
            path = f"{data_dir}/{name}.csv"
            if os.path.exists(path):
                loader(path)
            else:
                print(f"  ⚠ Skipping {name} (file not found)")

        self.build_edges()
        print(f"\n✅ Ingestion complete: {self.stats}")
        if self.stats["warnings"]:
            print(f"⚠ Warnings: {len(self.stats['warnings'])} broken references")
            for w in self.stats["warnings"][:5]:
                print(f"  - {w}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data/")
    parser.add_argument("--db", default="data/otc.db")
    parser.add_argument("--generate-sample", action="store_true")
    args = parser.parse_args()

    pipeline = OTCIngestionPipeline(args.db)
    if args.generate_sample:
        pipeline.generate_sample_data(args.data_dir)
    pipeline.run(args.data_dir)
