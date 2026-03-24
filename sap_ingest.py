"""
SAP Order-to-Cash JSONL → SQLite Graph Ingestion Pipeline
Handles your exact folder/file structure from sap-o2c-data/

USAGE:
  python sap_ingest.py --data-dir /path/to/sap-o2c-data --db otc.db

DATA FOLDER STRUCTURE EXPECTED:
  sap-o2c-data/
    sales_order_headers/        part-*.jsonl
    sales_order_items/          part-*.jsonl
    outbound_delivery_headers/  part-*.jsonl
    billing_document_cancellations/  part-*.jsonl
    payments_accounts_receivable/    part-*.jsonl
    journal_entry_items_accounts_receivable/  part-*.jsonl
    business_partners/          part-*.jsonl
    customer_company_assignments/    part-*.jsonl
    customer_sales_area_assignments/ part-*.jsonl
    product_descriptions/       part-*.jsonl
    product_plants/             part-*.jsonl
    product_storage_locations/  part-*.jsonl
    plants/                     part-*.jsonl
"""

import sqlite3, json, glob, os, argparse, sys
from pathlib import Path

# ── SCHEMA ────────────────────────────────────────────────────────────────────
SCHEMA = """
CREATE TABLE IF NOT EXISTS sales_order_headers (
    salesOrder TEXT PRIMARY KEY,
    salesOrderType TEXT, salesOrganization TEXT,
    distributionChannel TEXT, soldToParty TEXT,
    creationDate TEXT, totalNetAmount REAL,
    overallDeliveryStatus TEXT, overallOrdReltdBillgStatus TEXT,
    transactionCurrency TEXT, requestedDeliveryDate TEXT,
    customerPaymentTerms TEXT, headerBillingBlockReason TEXT,
    deliveryBlockReason TEXT, incotermsClassification TEXT,
    createdByUser TEXT, lastChangeDateTime TEXT
);

CREATE TABLE IF NOT EXISTS sales_order_items (
    salesOrder TEXT, salesOrderItem TEXT,
    material TEXT, requestedQuantity REAL,
    requestedQuantityUnit TEXT, netAmount REAL,
    transactionCurrency TEXT, materialGroup TEXT,
    productionPlant TEXT, storageLocation TEXT,
    salesOrderItemCategory TEXT,
    salesDocumentRjcnReason TEXT,
    PRIMARY KEY (salesOrder, salesOrderItem)
);

CREATE TABLE IF NOT EXISTS outbound_delivery_headers (
    deliveryDocument TEXT PRIMARY KEY,
    creationDate TEXT, shippingPoint TEXT,
    overallGoodsMovementStatus TEXT,
    overallPickingStatus TEXT,
    overallProofOfDeliveryStatus TEXT,
    actualGoodsMovementDate TEXT,
    deliveryBlockReason TEXT,
    headerBillingBlockReason TEXT,
    hdrGeneralIncompletionStatus TEXT,
    lastChangeDate TEXT
);

CREATE TABLE IF NOT EXISTS billing_documents (
    billingDocument TEXT PRIMARY KEY,
    billingDocumentType TEXT, creationDate TEXT,
    billingDocumentDate TEXT,
    billingDocumentIsCancelled INTEGER,
    cancelledBillingDocument TEXT,
    totalNetAmount REAL, transactionCurrency TEXT,
    companyCode TEXT, fiscalYear TEXT,
    accountingDocument TEXT, soldToParty TEXT,
    lastChangeDateTime TEXT
);

CREATE TABLE IF NOT EXISTS payments_ar (
    companyCode TEXT, fiscalYear TEXT,
    accountingDocument TEXT, accountingDocumentItem TEXT,
    clearingDate TEXT, clearingAccountingDocument TEXT,
    clearingDocFiscalYear TEXT,
    amountInTransactionCurrency REAL,
    transactionCurrency TEXT,
    amountInCompanyCodeCurrency REAL,
    companyCodeCurrency TEXT,
    customer TEXT, invoiceReference TEXT,
    salesDocument TEXT, postingDate TEXT,
    documentDate TEXT, glAccount TEXT,
    financialAccountType TEXT, profitCenter TEXT,
    PRIMARY KEY (companyCode, fiscalYear, accountingDocument, accountingDocumentItem)
);

CREATE TABLE IF NOT EXISTS journal_entry_items_ar (
    companyCode TEXT, fiscalYear TEXT,
    accountingDocument TEXT, accountingDocumentItem TEXT,
    glAccount TEXT, referenceDocument TEXT,
    costCenter TEXT, profitCenter TEXT,
    transactionCurrency TEXT,
    amountInTransactionCurrency REAL,
    companyCodeCurrency TEXT,
    amountInCompanyCodeCurrency REAL,
    postingDate TEXT, documentDate TEXT,
    accountingDocumentType TEXT,
    assignmentReference TEXT,
    customer TEXT, financialAccountType TEXT,
    clearingDate TEXT, clearingAccountingDocument TEXT,
    clearingDocFiscalYear TEXT,
    lastChangeDateTime TEXT,
    PRIMARY KEY (companyCode, fiscalYear, accountingDocument, accountingDocumentItem)
);

CREATE TABLE IF NOT EXISTS business_partners (
    businessPartner TEXT PRIMARY KEY,
    customer TEXT,
    businessPartnerCategory TEXT,
    businessPartnerFullName TEXT,
    businessPartnerName TEXT,
    businessPartnerGrouping TEXT,
    organizationBpName1 TEXT,
    organizationBpName2 TEXT,
    correspondenceLanguage TEXT,
    createdByUser TEXT, creationDate TEXT,
    lastChangeDate TEXT,
    businessPartnerIsBlocked INTEGER,
    isMarkedForArchiving INTEGER
);

CREATE TABLE IF NOT EXISTS customer_company_assignments (
    customer TEXT, companyCode TEXT,
    reconciliationAccount TEXT,
    accountByCustomer TEXT,
    paymentTerms TEXT,
    paymentMethodList TEXT,
    PRIMARY KEY (customer, companyCode)
);

CREATE TABLE IF NOT EXISTS customer_sales_area_assignments (
    customer TEXT, salesOrganization TEXT,
    distributionChannel TEXT, division TEXT,
    currency TEXT, customerPaymentTerms TEXT,
    shippingCondition TEXT,
    deliveryPriority TEXT,
    incotermsClassification TEXT,
    incotermsLocation1 TEXT,
    billingIsBlockedForCustomer TEXT,
    PRIMARY KEY (customer, salesOrganization, distributionChannel, division)
);

CREATE TABLE IF NOT EXISTS product_descriptions (
    product TEXT, language TEXT,
    productDescription TEXT,
    PRIMARY KEY (product, language)
);

CREATE TABLE IF NOT EXISTS product_plants (
    product TEXT, plant TEXT,
    profileCode TEXT,
    PRIMARY KEY (product, plant)
);

CREATE TABLE IF NOT EXISTS product_storage_locations (
    product TEXT, plant TEXT, storageLocation TEXT,
    PRIMARY KEY (product, plant, storageLocation)
);

CREATE TABLE IF NOT EXISTS plants (
    plant TEXT PRIMARY KEY,
    plantName TEXT,
    cityName TEXT,
    country TEXT,
    companyCode TEXT,
    factoryCalendar TEXT
);

-- GRAPH EDGE TABLE
CREATE TABLE IF NOT EXISTS edges (
    edge_id INTEGER PRIMARY KEY AUTOINCREMENT,
    from_type TEXT NOT NULL,
    from_id   TEXT NOT NULL,
    relationship TEXT NOT NULL,
    to_type   TEXT NOT NULL,
    to_id     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_e_from ON edges(from_type, from_id);
CREATE INDEX IF NOT EXISTS idx_e_to   ON edges(to_type,   to_id);
CREATE INDEX IF NOT EXISTS idx_e_rel  ON edges(relationship);
"""


# ── HELPERS ───────────────────────────────────────────────────────────────────
def load_jsonl(folder: str) -> list:
    """Load all part-*.jsonl files from a folder"""
    records = []
    pattern = os.path.join(folder, "part-*.jsonl")
    for path in sorted(glob.glob(pattern)):
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    return records


def sf(val):
    """Safe float"""
    try: return float(val) if val not in (None, "", "null") else None
    except: return None


def ss(val):
    """Safe string"""
    if val is None: return None
    return str(val).strip() or None


def sb(val):
    """Safe bool → int"""
    if isinstance(val, bool): return int(val)
    if isinstance(val, int): return val
    return None


def sd(val):
    """Safe date: strip to YYYY-MM-DD"""
    if not val: return None
    s = str(val)
    return s[:10] if len(s) >= 10 else s


class SAPIngestionPipeline:
    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path) if os.path.dirname(db_path) else ".", exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.executescript(SCHEMA)
        self.conn.commit()
        self.stats = {}

    def _upsert(self, table, cols, rows):
        if not rows: return 0
        placeholders = ",".join(["?"] * len(cols))
        col_str = ",".join(cols)
        sql = f"INSERT OR REPLACE INTO {table} ({col_str}) VALUES ({placeholders})"
        self.conn.executemany(sql, rows)
        self.conn.commit()
        return len(rows)

    def _edge(self, from_type, from_id, rel, to_type, to_id):
        if not from_id or not to_id: return
        self.conn.execute(
            "INSERT OR IGNORE INTO edges (from_type,from_id,relationship,to_type,to_id) VALUES (?,?,?,?,?)",
            (from_type, str(from_id), rel, to_type, str(to_id))
        )

    # ── LOADERS ───────────────────────────────────────────────────────────────

    def load_sales_order_headers(self, folder):
        recs = load_jsonl(folder)
        rows = []
        for r in recs:
            rows.append((
                ss(r.get("salesOrder")), ss(r.get("salesOrderType")),
                ss(r.get("salesOrganization")), ss(r.get("distributionChannel")),
                ss(r.get("soldToParty")),
                sd(r.get("creationDate")), sf(r.get("totalNetAmount")),
                ss(r.get("overallDeliveryStatus")),
                ss(r.get("overallOrdReltdBillgStatus")),
                ss(r.get("transactionCurrency")),
                sd(r.get("requestedDeliveryDate")),
                ss(r.get("customerPaymentTerms")),
                ss(r.get("headerBillingBlockReason")),
                ss(r.get("deliveryBlockReason")),
                ss(r.get("incotermsClassification")),
                ss(r.get("createdByUser")),
                sd(r.get("lastChangeDateTime"))
            ))
        n = self._upsert("sales_order_headers", [
            "salesOrder","salesOrderType","salesOrganization","distributionChannel",
            "soldToParty","creationDate","totalNetAmount","overallDeliveryStatus",
            "overallOrdReltdBillgStatus","transactionCurrency","requestedDeliveryDate",
            "customerPaymentTerms","headerBillingBlockReason","deliveryBlockReason",
            "incotermsClassification","createdByUser","lastChangeDateTime"
        ], [r for r in rows if r[0]])
        self.stats["sales_order_headers"] = n
        print(f"  ✓ {n:>5} sales_order_headers")
        return recs

    def load_sales_order_items(self, folder):
        recs = load_jsonl(folder)
        rows = [(
            ss(r.get("salesOrder")), ss(r.get("salesOrderItem")),
            ss(r.get("material")), sf(r.get("requestedQuantity")),
            ss(r.get("requestedQuantityUnit")), sf(r.get("netAmount")),
            ss(r.get("transactionCurrency")), ss(r.get("materialGroup")),
            ss(r.get("productionPlant")), ss(r.get("storageLocation")),
            ss(r.get("salesOrderItemCategory")), ss(r.get("salesDocumentRjcnReason"))
        ) for r in recs]
        n = self._upsert("sales_order_items", [
            "salesOrder","salesOrderItem","material","requestedQuantity",
            "requestedQuantityUnit","netAmount","transactionCurrency","materialGroup",
            "productionPlant","storageLocation","salesOrderItemCategory","salesDocumentRjcnReason"
        ], [r for r in rows if r[0] and r[1]])
        self.stats["sales_order_items"] = n
        print(f"  ✓ {n:>5} sales_order_items")

    def load_delivery_headers(self, folder):
        recs = load_jsonl(folder)
        rows = [(
            ss(r.get("deliveryDocument")),
            sd(r.get("creationDate")), ss(r.get("shippingPoint")),
            ss(r.get("overallGoodsMovementStatus")),
            ss(r.get("overallPickingStatus")),
            ss(r.get("overallProofOfDeliveryStatus")),
            sd(r.get("actualGoodsMovementDate")),
            ss(r.get("deliveryBlockReason")),
            ss(r.get("headerBillingBlockReason")),
            ss(r.get("hdrGeneralIncompletionStatus")),
            sd(r.get("lastChangeDate"))
        ) for r in recs]
        n = self._upsert("outbound_delivery_headers", [
            "deliveryDocument","creationDate","shippingPoint",
            "overallGoodsMovementStatus","overallPickingStatus",
            "overallProofOfDeliveryStatus","actualGoodsMovementDate",
            "deliveryBlockReason","headerBillingBlockReason",
            "hdrGeneralIncompletionStatus","lastChangeDate"
        ], [r for r in rows if r[0]])
        self.stats["outbound_delivery_headers"] = n
        print(f"  ✓ {n:>5} outbound_delivery_headers")

    def load_billing_documents(self, folder):
        recs = load_jsonl(folder)
        rows = [(
            ss(r.get("billingDocument")),
            ss(r.get("billingDocumentType")),
            sd(r.get("creationDate")), sd(r.get("billingDocumentDate")),
            sb(r.get("billingDocumentIsCancelled")),
            ss(r.get("cancelledBillingDocument")),
            sf(r.get("totalNetAmount")),
            ss(r.get("transactionCurrency")),
            ss(r.get("companyCode")), ss(r.get("fiscalYear")),
            ss(r.get("accountingDocument")),
            ss(r.get("soldToParty")),
            sd(r.get("lastChangeDateTime"))
        ) for r in recs]
        n = self._upsert("billing_documents", [
            "billingDocument","billingDocumentType","creationDate","billingDocumentDate",
            "billingDocumentIsCancelled","cancelledBillingDocument","totalNetAmount",
            "transactionCurrency","companyCode","fiscalYear","accountingDocument",
            "soldToParty","lastChangeDateTime"
        ], [r for r in rows if r[0]])
        self.stats["billing_documents"] = n
        print(f"  ✓ {n:>5} billing_documents")

    def load_payments_ar(self, folder):
        recs = load_jsonl(folder)
        rows = [(
            ss(r.get("companyCode")), ss(r.get("fiscalYear")),
            ss(r.get("accountingDocument")), ss(r.get("accountingDocumentItem")),
            sd(r.get("clearingDate")), ss(r.get("clearingAccountingDocument")),
            ss(r.get("clearingDocFiscalYear")),
            sf(r.get("amountInTransactionCurrency")),
            ss(r.get("transactionCurrency")),
            sf(r.get("amountInCompanyCodeCurrency")),
            ss(r.get("companyCodeCurrency")),
            ss(r.get("customer")), ss(r.get("invoiceReference")),
            ss(r.get("salesDocument")),
            sd(r.get("postingDate")), sd(r.get("documentDate")),
            ss(r.get("glAccount")), ss(r.get("financialAccountType")),
            ss(r.get("profitCenter"))
        ) for r in recs]
        n = self._upsert("payments_ar", [
            "companyCode","fiscalYear","accountingDocument","accountingDocumentItem",
            "clearingDate","clearingAccountingDocument","clearingDocFiscalYear",
            "amountInTransactionCurrency","transactionCurrency",
            "amountInCompanyCodeCurrency","companyCodeCurrency",
            "customer","invoiceReference","salesDocument",
            "postingDate","documentDate","glAccount","financialAccountType","profitCenter"
        ], [r for r in rows if r[0]])
        self.stats["payments_ar"] = n
        print(f"  ✓ {n:>5} payments_ar")

    def load_journal_entries(self, folder):
        recs = load_jsonl(folder)
        rows = [(
            ss(r.get("companyCode")), ss(r.get("fiscalYear")),
            ss(r.get("accountingDocument")), ss(r.get("accountingDocumentItem")),
            ss(r.get("glAccount")), ss(r.get("referenceDocument")),
            ss(r.get("costCenter")), ss(r.get("profitCenter")),
            ss(r.get("transactionCurrency")),
            sf(r.get("amountInTransactionCurrency")),
            ss(r.get("companyCodeCurrency")),
            sf(r.get("amountInCompanyCodeCurrency")),
            sd(r.get("postingDate")), sd(r.get("documentDate")),
            ss(r.get("accountingDocumentType")),
            ss(r.get("assignmentReference")),
            ss(r.get("customer")), ss(r.get("financialAccountType")),
            sd(r.get("clearingDate")), ss(r.get("clearingAccountingDocument")),
            ss(r.get("clearingDocFiscalYear")), sd(r.get("lastChangeDateTime"))
        ) for r in recs]
        n = self._upsert("journal_entry_items_ar", [
            "companyCode","fiscalYear","accountingDocument","accountingDocumentItem",
            "glAccount","referenceDocument","costCenter","profitCenter",
            "transactionCurrency","amountInTransactionCurrency",
            "companyCodeCurrency","amountInCompanyCodeCurrency",
            "postingDate","documentDate","accountingDocumentType","assignmentReference",
            "customer","financialAccountType","clearingDate",
            "clearingAccountingDocument","clearingDocFiscalYear","lastChangeDateTime"
        ], [r for r in rows if r[0]])
        self.stats["journal_entry_items_ar"] = n
        print(f"  ✓ {n:>5} journal_entry_items_ar")

    def load_business_partners(self, folder):
        recs = load_jsonl(folder)
        rows = [(
            ss(r.get("businessPartner")), ss(r.get("customer")),
            ss(r.get("businessPartnerCategory")),
            ss(r.get("businessPartnerFullName")),
            ss(r.get("businessPartnerName")),
            ss(r.get("businessPartnerGrouping")),
            ss(r.get("organizationBpName1")),
            ss(r.get("organizationBpName2")),
            ss(r.get("correspondenceLanguage")),
            ss(r.get("createdByUser")),
            sd(r.get("creationDate")), sd(r.get("lastChangeDate")),
            sb(r.get("businessPartnerIsBlocked")),
            sb(r.get("isMarkedForArchiving"))
        ) for r in recs]
        n = self._upsert("business_partners", [
            "businessPartner","customer","businessPartnerCategory",
            "businessPartnerFullName","businessPartnerName","businessPartnerGrouping",
            "organizationBpName1","organizationBpName2","correspondenceLanguage",
            "createdByUser","creationDate","lastChangeDate",
            "businessPartnerIsBlocked","isMarkedForArchiving"
        ], [r for r in rows if r[0]])
        self.stats["business_partners"] = n
        print(f"  ✓ {n:>5} business_partners")

    def load_customer_company(self, folder):
        recs = load_jsonl(folder)
        rows = [(
            ss(r.get("customer")), ss(r.get("companyCode")),
            ss(r.get("reconciliationAccount")),
            ss(r.get("accountByCustomer")),
            ss(r.get("paymentTerms")),
            ss(r.get("paymentMethodList"))
        ) for r in recs]
        n = self._upsert("customer_company_assignments", [
            "customer","companyCode","reconciliationAccount",
            "accountByCustomer","paymentTerms","paymentMethodList"
        ], [r for r in rows if r[0]])
        self.stats["customer_company_assignments"] = n
        print(f"  ✓ {n:>5} customer_company_assignments")

    def load_customer_sales_area(self, folder):
        recs = load_jsonl(folder)
        rows = [(
            ss(r.get("customer")), ss(r.get("salesOrganization")),
            ss(r.get("distributionChannel")), ss(r.get("division")),
            ss(r.get("currency")), ss(r.get("customerPaymentTerms")),
            ss(r.get("shippingCondition")), ss(r.get("deliveryPriority")),
            ss(r.get("incotermsClassification")), ss(r.get("incotermsLocation1")),
            ss(r.get("billingIsBlockedForCustomer"))
        ) for r in recs]
        n = self._upsert("customer_sales_area_assignments", [
            "customer","salesOrganization","distributionChannel","division",
            "currency","customerPaymentTerms","shippingCondition","deliveryPriority",
            "incotermsClassification","incotermsLocation1","billingIsBlockedForCustomer"
        ], [r for r in rows if r[0]])
        self.stats["customer_sales_area_assignments"] = n
        print(f"  ✓ {n:>5} customer_sales_area_assignments")

    def load_product_descriptions(self, folder):
        recs = load_jsonl(folder)
        rows = [(ss(r.get("product")), ss(r.get("language")), ss(r.get("productDescription"))) for r in recs]
        n = self._upsert("product_descriptions", ["product","language","productDescription"],
                         [r for r in rows if r[0]])
        self.stats["product_descriptions"] = n
        print(f"  ✓ {n:>5} product_descriptions")

    def load_product_plants(self, folder):
        recs = load_jsonl(folder)
        rows = [(ss(r.get("product")), ss(r.get("plant")), ss(r.get("profileCode"))) for r in recs]
        n = self._upsert("product_plants", ["product","plant","profileCode"],
                         [r for r in rows if r[0]])
        self.stats["product_plants"] = n
        print(f"  ✓ {n:>5} product_plants")

    def load_product_storage(self, folder):
        recs = load_jsonl(folder)
        rows = [(ss(r.get("product")), ss(r.get("plant")), ss(r.get("storageLocation"))) for r in recs]
        n = self._upsert("product_storage_locations", ["product","plant","storageLocation"],
                         [r for r in rows if r[0]])
        self.stats["product_storage_locations"] = n
        print(f"  ✓ {n:>5} product_storage_locations")

    def load_plants(self, folder):
        recs = load_jsonl(folder)
        rows = [(ss(r.get("plant")), ss(r.get("plantName")), ss(r.get("cityName")),
                 ss(r.get("country")), ss(r.get("companyCode")), ss(r.get("factoryCalendar")))
                for r in recs]
        n = self._upsert("plants", ["plant","plantName","cityName","country","companyCode","factoryCalendar"],
                         [r for r in rows if r[0]])
        self.stats["plants"] = n
        print(f"  ✓ {n:>5} plants")

    # ── EDGE BUILDER ──────────────────────────────────────────────────────────
    def build_edges(self):
        """
        Build graph edges from FK relationships between SAP entities.

        KEY LINKS IN YOUR DATA:
          salesOrder.soldToParty → business_partners.businessPartner
          salesOrder → billing_documents via accountingDocument chain
          billing_documents.accountingDocument → journal_entry_items_ar.accountingDocument
          journal_entry_items_ar.referenceDocument → billing_documents.billingDocument
          billing_documents.soldToParty → business_partners.businessPartner
          payments_ar.accountingDocument → journal_entry_items_ar.accountingDocument
          payments_ar.customer → business_partners.businessPartner
          sales_order_items.material → product_descriptions.product
          sales_order_items.productionPlant → plants.plant
        """
        print("\n  Building graph edges...")
        self.conn.execute("DELETE FROM edges")  # fresh rebuild
        self.conn.commit()
        count = 0

        cur = self.conn.cursor()

        # SALES ORDER → CUSTOMER (PLACED_BY)
        cur.execute("SELECT salesOrder, soldToParty FROM sales_order_headers WHERE soldToParty IS NOT NULL")
        for row in cur.fetchall():
            self._edge("sales_order", row[0], "PLACED_BY", "business_partner", row[1])
            count += 1

        # BILLING → CUSTOMER (BILLED_TO)
        cur.execute("SELECT billingDocument, soldToParty FROM billing_documents WHERE soldToParty IS NOT NULL")
        for row in cur.fetchall():
            self._edge("billing_document", row[0], "BILLED_TO", "business_partner", row[1])
            count += 1

        # BILLING → JOURNAL ENTRY (POSTED_AS)
        # Link: billing.accountingDocument = journal.accountingDocument
        cur.execute("""
            SELECT DISTINCT b.billingDocument, j.accountingDocument, j.accountingDocumentItem
            FROM billing_documents b
            JOIN journal_entry_items_ar j ON b.accountingDocument = j.accountingDocument
        """)
        for row in cur.fetchall():
            self._edge("billing_document", row[0], "POSTED_AS",
                       "journal_entry", row[1] + "_" + row[2])
            count += 1

        # JOURNAL → BILLING (reverse link via referenceDocument)
        # journal.referenceDocument = billing.billingDocument
        cur.execute("""
            SELECT DISTINCT j.accountingDocument, j.accountingDocumentItem, j.referenceDocument
            FROM journal_entry_items_ar j
            WHERE j.referenceDocument IS NOT NULL
        """)
        for row in cur.fetchall():
            jid = row[0] + "_" + row[1]
            self._edge("journal_entry", jid, "REFERENCES_BILLING",
                       "billing_document", row[2])
            count += 1

        # PAYMENT → JOURNAL (CLEARS)
        # payment.accountingDocument → journal.accountingDocument
        cur.execute("""
            SELECT DISTINCT p.accountingDocument, p.accountingDocumentItem, p.accountingDocument
            FROM payments_ar p
        """)
        for row in cur.fetchall():
            pid = row[0] + "_" + row[1]
            self._edge("payment", pid, "CLEARS",
                       "journal_entry", row[2] + "_1")
            count += 1

        # PAYMENT → CUSTOMER
        cur.execute("SELECT DISTINCT accountingDocument, accountingDocumentItem, customer FROM payments_ar WHERE customer IS NOT NULL")
        for row in cur.fetchall():
            pid = row[0] + "_" + row[1]
            self._edge("payment", pid, "PAID_BY", "business_partner", row[2])
            count += 1

        # SALES ORDER → MATERIAL/PRODUCT
        cur.execute("SELECT DISTINCT salesOrder, material FROM sales_order_items WHERE material IS NOT NULL")
        for row in cur.fetchall():
            self._edge("sales_order", row[0], "CONTAINS_MATERIAL", "material", row[1])
            count += 1

        # SALES ORDER → DELIVERY (via shippingPoint/plant inference)
        # NOTE: your delivery headers don't carry salesOrder directly
        # We match on creationDate + shippingPoint → productionPlant as best-effort
        # This is a known SAP data gap — direct link needs delivery items table
        cur.execute("""
            SELECT DISTINCT s.salesOrder, d.deliveryDocument
            FROM sales_order_headers s
            JOIN outbound_delivery_headers d
              ON s.creationDate = d.creationDate
        """)
        for row in cur.fetchall():
            self._edge("sales_order", row[0], "HAS_DELIVERY", "delivery", row[1])
            count += 1

        # MATERIAL → PLANT
        cur.execute("SELECT DISTINCT product, plant FROM product_plants WHERE product IS NOT NULL AND plant IS NOT NULL")
        for row in cur.fetchall():
            self._edge("material", row[0], "STORED_AT", "plant", row[1])
            count += 1

        self.conn.commit()
        self.stats["edges"] = count
        print(f"  ✓ {count:>5} edges built")

    def print_summary(self):
        print("\n" + "─"*52)
        print("  INGESTION SUMMARY")
        print("─"*52)
        for k, v in self.stats.items():
            print(f"  {k:<42} {v:>6}")
        total = sum(v for k,v in self.stats.items() if k != "edges")
        print("─"*52)
        print(f"  {'TOTAL RECORDS':<42} {total:>6}")
        print(f"  {'GRAPH EDGES':<42} {self.stats.get('edges',0):>6}")
        print("─"*52)

    def get_schema_for_llm(self) -> str:
        """Returns schema string for injection into LLM prompts"""
        lines = []
        cur = self.conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name != 'edges' ORDER BY name")
        for (tname,) in cur.fetchall():
            cols = self.conn.execute(f"PRAGMA table_info({tname})").fetchall()
            col_str = ", ".join(c[1] for c in cols)
            lines.append(f"  {tname}({col_str})")
        return "\n".join(lines)

    def run(self, data_dir: str):
        base = data_dir.rstrip("/")
        print(f"\n🚀 SAP OTC Ingestion Pipeline")
        print(f"   Source: {base}")
        print(f"   Target: {self.db_path}\n")

        folder_map = {
            "sales_order_headers":        self.load_sales_order_headers,
            "sales_order_items":          self.load_sales_order_items,
            "outbound_delivery_headers":  self.load_delivery_headers,
            "billing_document_cancellations": self.load_billing_documents,
            "payments_accounts_receivable":   self.load_payments_ar,
            "journal_entry_items_accounts_receivable": self.load_journal_entries,
            "business_partners":          self.load_business_partners,
            "customer_company_assignments":    self.load_customer_company,
            "customer_sales_area_assignments": self.load_customer_sales_area,
            "product_descriptions":       self.load_product_descriptions,
            "product_plants":             self.load_product_plants,
            "product_storage_locations":  self.load_product_storage,
            "plants":                     self.load_plants,
        }

        for folder_name, loader in folder_map.items():
            folder_path = os.path.join(base, folder_name)
            if os.path.isdir(folder_path):
                loader(folder_path)
            else:
                print(f"  ⚠  Skipping {folder_name} (not found)")

        self.build_edges()
        self.print_summary()
        print(f"\n✅ Database ready: {self.db_path}")
        print(f"   LLM Schema preview:")
        print(self.get_schema_for_llm()[:600])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SAP OTC JSONL → SQLite ingestion")
    parser.add_argument("--data-dir", required=True, help="Path to sap-o2c-data/ folder")
    parser.add_argument("--db", default="data/otc.db", help="Output SQLite DB path")
    args = parser.parse_args()
    p = SAPIngestionPipeline(args.db)
    p.run(args.data_dir)
