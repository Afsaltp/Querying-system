"""
Order-to-Cash Graph Query System - FastAPI Backend
Architecture: FastAPI + SQLite (hybrid relational+graph) + Groq LLM
"""
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional
import uvicorn

from api.query_engine import QueryEngine
from api.graph_api import GraphAPI
from graph.graph_store import GraphStore

app = FastAPI(title="OTC Graph Query System", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize core components
graph_store = GraphStore("data/otc.db")
query_engine = QueryEngine(graph_store)
graph_api = GraphAPI(graph_store)


class QueryRequest(BaseModel):
    query: str
    session_id: Optional[str] = "default"


class NodeRequest(BaseModel):
    node_id: str
    node_type: str


class ExpandRequest(BaseModel):
    node_id: str
    node_type: str
    depth: int = 1


@app.post("/query")
async def query(req: QueryRequest):
    """Natural language query → SQL/Graph → Grounded response"""
    try:
        result = await query_engine.process(req.query, req.session_id)
        return JSONResponse(result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/graph/node")
async def get_node(req: NodeRequest):
    """Get single node with its direct relationships"""
    try:
        node = graph_api.get_node(req.node_id, req.node_type)
        if not node:
            raise HTTPException(status_code=404, detail="Node not found")
        return JSONResponse(node)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/graph/expand")
async def expand_node(req: ExpandRequest):
    """Multi-hop graph expansion from a node"""
    try:
        subgraph = graph_api.expand_node(req.node_id, req.node_type, req.depth)
        return JSONResponse(subgraph)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/graph/full")
async def get_full_graph():
    """Return full graph for initial visualization"""
    try:
        graph = graph_api.get_full_graph()
        return JSONResponse(graph)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/graph/broken-flows")
async def get_broken_flows():
    """Detect incomplete Order-to-Cash flows"""
    try:
        broken = graph_api.detect_broken_flows()
        return JSONResponse(broken)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health():
    return {"status": "ok", "nodes": graph_store.node_count()}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
