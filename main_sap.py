"""
OTC Graph System — FastAPI Backend (SAP Edition)
"""
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional
import sqlite3, os, uvicorn

SAP_DB  = os.getenv("DB_PATH", "data/otc.db")
DB_PATH = SAP_DB if os.path.exists(SAP_DB) else "data/sample_otc.db"

app = FastAPI(title="SAP OTC Graph Query System", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

_query_engine = None
_graph_api    = None

def get_query_engine():
    global _query_engine
    if _query_engine is None:
        from sap_query_engine import SAPQueryEngine
        _query_engine = SAPQueryEngine(DB_PATH)
    return _query_engine

def get_graph_api():
    global _graph_api
    if _graph_api is None:
        from sap_graph_api import SAPGraphAPI
        _graph_api = SAPGraphAPI(DB_PATH)
    return _graph_api

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

@app.post("/query")
async def query(req: QueryRequest):
    try:
        result = await get_query_engine().process(req.query, req.session_id)
        return JSONResponse(result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/graph/node")
async def get_node(req: NodeRequest):
    try:
        node = get_graph_api().get_node(req.node_id, req.node_type)
        if not node: raise HTTPException(status_code=404, detail="Node not found")
        return JSONResponse(node)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/graph/expand")
async def expand_node(req: ExpandRequest):
    try:
        return JSONResponse(get_graph_api().expand_node(req.node_id, req.node_type, req.depth))
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
    conn = sqlite3.connect(DB_PATH)
    counts = {}
    for t in ["sales_order_headers","billing_documents","payments_ar","business_partners"]:
        try: counts[t] = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        except: pass
    conn.close()
    return {"status": "ok", "db": DB_PATH, "counts": counts}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
