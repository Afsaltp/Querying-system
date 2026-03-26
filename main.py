"""
OTC Graph System — FastAPI Backend
All imports are flat (same folder). No subfolders needed.
Run: uvicorn main:app --reload --port 8000
"""
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from pydantic import BaseModel
from typing import Optional
import sqlite3, os, uvicorn

# ── DB PATH ──────────────────────────────────────────────────────────────────
# Put otc.db in same folder as this file, OR set DB_PATH env variable
DB_PATH = os.getenv("DB_PATH", "otc.db")
if not os.path.exists(DB_PATH):
    # try data subfolder
    DB_PATH = os.path.join("data", "otc.db")

app = FastAPI(title="SAP OTC Graph Query System", version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Lazy-loaded singletons
_query_engine = None
_graph_api = None

FRONTEND_PATH = os.path.join(os.path.dirname(__file__), "frontend.html")


def _frontend_html() -> str:
    # Serve the single-page UI from the same backend so public tunnels work reliably.
    with open(FRONTEND_PATH, "r", encoding="utf-8") as f:
        return f.read()

def get_query_engine():
    global _query_engine
    if _query_engine is None:
        from sap_query_engine import SAPQueryEngine   # flat import
        _query_engine = SAPQueryEngine(DB_PATH)
    return _query_engine

def get_graph_api():
    global _graph_api
    if _graph_api is None:
        from sap_graph_api import SAPGraphAPI         # flat import
        _graph_api = SAPGraphAPI(DB_PATH)
    return _graph_api


# ── REQUEST MODELS ────────────────────────────────────────────────────────────
class QueryRequest(BaseModel):
    query: str
    session_id: Optional[str] = "default"

class NodeRequest(BaseModel):
    node_id: str
    node_type: str

class ExpandRequest(BaseModel):
    node_id: str
    node_type: str
    depth: int = 2


# ── ROUTES ────────────────────────────────────────────────────────────────────
@app.post("/query")
async def query_endpoint(req: QueryRequest):
    try:
        result = await get_query_engine().process(req.query, req.session_id)
        return JSONResponse(result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/graph/node")
async def get_node(req: NodeRequest):
    try:
        node = get_graph_api().get_node(req.node_id, req.node_type)
        if not node:
            raise HTTPException(status_code=404, detail="Node not found")
        return JSONResponse(node)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/graph/expand")
async def expand_node(req: ExpandRequest):
    try:
        result = get_graph_api().expand_node(req.node_id, req.node_type, req.depth)
        return JSONResponse(result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/graph/full")
async def full_graph():
    try:
        return JSONResponse(get_graph_api().get_full_graph())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/graph/broken-flows")
async def broken_flows():
    try:
        return JSONResponse(get_graph_api().detect_broken_flows())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health():
    try:
        conn = sqlite3.connect(DB_PATH)
        counts = {}
        for t in ["sales_order_headers", "billing_documents",
                  "payments_ar", "business_partners", "edges"]:
            try:
                counts[t] = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            except:
                counts[t] = 0
        conn.close()
        return {"status": "ok", "db": DB_PATH, "counts": counts}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


@app.get("/")
async def root():
    try:
        return HTMLResponse(_frontend_html())
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Frontend missing: {e}")


@app.get("/frontend.html")
async def frontend():
    try:
        return HTMLResponse(_frontend_html())
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Frontend missing: {e}")


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
