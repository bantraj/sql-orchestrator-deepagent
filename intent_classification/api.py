"""
api.py  –  Enhanced FastAPI Intent Classification Service
==========================================================
New in v2:
  • POST /upload          Upload CSV / Excel / PDF to build the index
  • POST /predict         Accepts strategy & top_k parameters
  • POST /predict/batch   Accepts strategy & top_k parameters
  • GET  /health          Shows loaded file metadata + strategy
  • GET  /intents         Lists all known intents (filterable by area)

Run:
  uvicorn api:app --host 0.0.0.0 --port 8000 --reload
"""

from __future__ import annotations

import os
import time
from contextlib import asynccontextmanager
from typing import Literal, Optional

from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from core import IntentRAGEngine, SearchStrategy

# ─────────────────────────────────────────────────────────────────────────────
# Config from environment
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_EXCEL   = os.environ.get("EXCEL_PATH",     "Intent_Training_Data.xlsx")
LLM_PROVIDER    = os.environ.get("LLM_PROVIDER",   "gemini")
GOOGLE_KEY      = os.environ.get("GOOGLE_API_KEY",  "")
OPENAI_KEY      = os.environ.get("OPENAI_API_KEY",  "")
DEFAULT_ALPHA   = float(os.environ.get("HYBRID_ALPHA", "0.6"))
DEFAULT_STRATEGY: SearchStrategy = os.environ.get("SEARCH_STRATEGY", "hybrid")  # type: ignore
PERSIST_DIR     = os.environ.get("CHROMA_DIR",      "./chroma_db")

# ─────────────────────────────────────────────────────────────────────────────
# App state
# ─────────────────────────────────────────────────────────────────────────────

engine:        Optional[IntentRAGEngine] = None
startup_error: Optional[str]            = None
startup_time:  Optional[float]          = None
loaded_file:   str                      = ""


@asynccontextmanager
async def lifespan(app: FastAPI):
    global engine, startup_error, startup_time, loaded_file
    # Auto-load default Excel if it exists
    if os.path.exists(DEFAULT_EXCEL):
        t0 = time.time()
        try:
            api_key = GOOGLE_KEY if LLM_PROVIDER == "gemini" else OPENAI_KEY
            engine  = IntentRAGEngine.from_file(
                source=DEFAULT_EXCEL, file_name=DEFAULT_EXCEL,
                llm_provider=LLM_PROVIDER, api_key=api_key,
                alpha=DEFAULT_ALPHA, persist_dir=PERSIST_DIR,
                strategy=DEFAULT_STRATEGY,
            )
            loaded_file  = DEFAULT_EXCEL
            startup_time = round(time.time() - t0, 2)
            print(f"✅ Auto-loaded '{DEFAULT_EXCEL}' in {startup_time}s")
        except Exception as exc:
            startup_error = str(exc)
            print(f"⚠️  Auto-load failed: {exc}")
    else:
        print(f"ℹ️  No default file found at '{DEFAULT_EXCEL}'. Use POST /upload.")
    yield
    print("🛑 Shutting down")


# ─────────────────────────────────────────────────────────────────────────────
# App
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Intent RAG API v2",
    description=(
        "Hybrid RAG intent classification with **file upload**, "
        "**search strategy selection** (simple or hybrid), and **configurable top-k**.\n\n"
        "Powered by **LangChain** + **ChromaDB** + **Gemini 1.5 Flash** / **GPT-4o-mini**."
    ),
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)


# ─────────────────────────────────────────────────────────────────────────────
# Pydantic schemas
# ─────────────────────────────────────────────────────────────────────────────

class ScoreBreakdown(BaseModel):
    hybrid:   Optional[float]
    semantic: float
    bm25:     Optional[float]


class RetrievedDoc(BaseModel):
    rank:           int
    utterance:      str
    intent:         str
    area:           str
    strategy:       str
    hybrid_score:   Optional[float]
    semantic_score: float
    bm25_score:     Optional[float]


class PredictRequest(BaseModel):
    query:    str           = Field(..., min_length=2, max_length=500,
                                   example="How do I reset my password?")
    strategy: Optional[Literal["hybrid", "simple"]] = Field(
        None, description="Override the engine default. null = use engine default."
    )
    top_k:    int           = Field(1, ge=1, le=10,
                                   description="Number of documents to retrieve (1-10).")


class PredictResponse(BaseModel):
    query:               str
    intent:              str
    confidence:          Literal["high", "medium", "low"]
    reasoning:           str
    area:                str
    strategy:            str
    top_k:               int
    retrieved_utterance: str
    retrieved_docs:      list[RetrievedDoc]
    scores:              ScoreBreakdown
    latency_ms:          int


class BatchRequest(BaseModel):
    queries:  list[str]     = Field(..., min_length=1, max_length=50)
    strategy: Optional[Literal["hybrid", "simple"]] = None
    top_k:    int           = Field(1, ge=1, le=10)


class BatchItem(PredictResponse):
    pass


class BatchResponse(BaseModel):
    results:    list[BatchItem]
    total:      int
    strategy:   str
    top_k:      int
    latency_ms: int


class UploadResponse(BaseModel):
    status:       str
    file_name:    str
    total_docs:   int
    intent_count: int
    area_count:   int
    strategy:     str
    alpha:        float
    build_time_s: float


class HealthResponse(BaseModel):
    status:          str
    loaded_file:     str
    llm_provider:    str
    embedding_model: str
    total_docs:      int
    strategy:        str
    alpha:           float
    startup_time:    Optional[float]
    error:           Optional[str]


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _require_engine():
    if engine is None:
        detail = (
            f"Engine not ready. Use POST /upload to load a file."
            + (f" Startup error: {startup_error}" if startup_error else "")
        )
        raise HTTPException(status_code=503, detail=detail)


def _build_response(result: dict, ms: int) -> PredictResponse:
    hits = result["retrieved"]
    top  = hits[0]
    return PredictResponse(
        query=result["query"],
        intent=result["intent"],
        confidence=result["confidence"],
        reasoning=result["reasoning"],
        area=result["area"],
        strategy=result["strategy"],
        top_k=result["top_k"],
        retrieved_utterance=result["retrieved_utterance"],
        retrieved_docs=[
            RetrievedDoc(
                rank=h["rank"],
                utterance=h["utterance"],
                intent=h["intent"],
                area=h["area"],
                strategy=h["strategy"],
                hybrid_score=h.get("hybrid_score"),
                semantic_score=h.get("semantic_score", 0.0),
                bm25_score=h.get("bm25_score"),
            )
            for h in hits
        ],
        scores=ScoreBreakdown(
            hybrid=top.get("hybrid_score"),
            semantic=top.get("semantic_score", 0.0),
            bm25=top.get("bm25_score"),
        ),
        latency_ms=ms,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/", tags=["Info"])
async def root():
    return {
        "service": "Intent RAG API",
        "version": "2.0.0",
        "docs":    "/docs",
        "upload":  "POST /upload",
        "predict": "POST /predict",
    }


@app.get("/health", response_model=HealthResponse, tags=["Info"])
async def health():
    _embed = lambda p: "text-embedding-004" if p == "gemini" else "text-embedding-3-small"
    if engine is None:
        return HealthResponse(
            status="no_engine", loaded_file="",
            llm_provider=LLM_PROVIDER,
            embedding_model=_embed(LLM_PROVIDER),
            total_docs=0,
            strategy=DEFAULT_STRATEGY, alpha=DEFAULT_ALPHA,
            startup_time=startup_time, error=startup_error,
        )
    return HealthResponse(
        status="ok", loaded_file=loaded_file,
        llm_provider=engine.provider,
        embedding_model=_embed(engine.provider),
        total_docs=engine.total_docs,
        strategy=engine.strategy, alpha=engine.alpha,
        startup_time=startup_time, error=None,
    )


@app.post("/upload", response_model=UploadResponse, tags=["Data"])
async def upload_file(
    file:     UploadFile = File(..., description="CSV, Excel (.xlsx/.xls), or PDF"),
    provider: str        = Form("gemini",  description="gemini | openai"),
    api_key:  str        = Form("",        description="LLM API key (or set env var)"),
    strategy: str        = Form("hybrid",  description="hybrid | simple"),
    alpha:    float      = Form(0.6,       description="Semantic weight (0.0-1.0)"),
):
    """
    Upload a CSV / Excel / PDF file to build the intent index.
    Replaces any previously loaded data.

    - **CSV / Excel**: must have `Utterance` and `Intent` columns (and optionally `Area`)
    - **PDF**: each non-trivial line becomes an utterance (Intent left blank)
    """
    global engine, loaded_file

    allowed = {".csv", ".xlsx", ".xls", ".pdf"}
    ext     = "." + (file.filename or "").rsplit(".", 1)[-1].lower()
    if ext not in allowed:
        raise HTTPException(400, f"Unsupported file type '{ext}'. Allowed: {allowed}")

    if strategy not in ("hybrid", "simple"):
        raise HTTPException(400, f"strategy must be 'hybrid' or 'simple', got '{strategy}'")

    alpha = max(0.0, min(1.0, alpha))

    file_bytes = await file.read()
    t0 = time.time()
    try:
        key = api_key or (
            GOOGLE_KEY if provider == "gemini" else OPENAI_KEY
        )
        eng = IntentRAGEngine.from_file(
            source=file_bytes, file_name=file.filename or "",
            llm_provider=provider, api_key=key,
            alpha=alpha, persist_dir=PERSIST_DIR,
            strategy=strategy,                      # type: ignore[arg-type]
        )
        engine      = eng
        loaded_file = file.filename or ""
        build_time  = round(time.time() - t0, 2)
        return UploadResponse(
            status="ok",
            file_name=loaded_file,
            total_docs=eng.total_docs,
            intent_count=len(eng.intent_list),
            area_count=len(eng.area_list),
            strategy=eng.strategy,
            alpha=eng.alpha,
            build_time_s=build_time,
        )
    except Exception as exc:
        raise HTTPException(500, f"Failed to build index: {exc}")


@app.post("/predict", response_model=PredictResponse, tags=["Classification"])
async def predict(body: PredictRequest):
    """
    Classify a single user query.

    - **strategy**: `hybrid` (BM25 + semantic) or `simple` (semantic only).
      Omit to use the engine default.
    - **top_k**: how many training utterances to retrieve before LLM reasoning (1-10).
    """
    _require_engine()
    t0     = time.time()
    try:
        result = engine.predict(             # type: ignore[union-attr]
            query=body.query,
            strategy=body.strategy,          # None → engine default
            top_k=body.top_k,
        )
    except Exception as exc:
        raise HTTPException(500, str(exc))
    ms = int((time.time() - t0) * 1000)
    return _build_response(result, ms)


@app.post("/predict/batch", response_model=BatchResponse, tags=["Classification"])
async def predict_batch(body: BatchRequest):
    """
    Classify up to 50 queries in one call.
    All queries use the same `strategy` and `top_k`.
    """
    _require_engine()
    t0, results = time.time(), []
    for q in body.queries:
        try:
            r  = engine.predict(q, strategy=body.strategy, top_k=body.top_k)   # type: ignore
            ms = int((time.time() - t0) * 1000)
            results.append(_build_response(r, ms))
        except Exception as exc:
            raise HTTPException(500, f"Error on query '{q}': {exc}")

    total_ms = int((time.time() - t0) * 1000)
    used_strategy = results[0].strategy if results else (body.strategy or "hybrid")
    return BatchResponse(
        results=results, total=len(results),
        strategy=used_strategy, top_k=body.top_k,
        latency_ms=total_ms,
    )


@app.get("/intents", tags=["Info"])
async def list_intents(
    area: Optional[str] = Query(None, description="Filter by area name"),
):
    """List all intent labels. Optionally filter by area."""
    _require_engine()
    df = engine.df                        # type: ignore[union-attr]
    if area:
        df = df[df["Area"].str.lower() == area.lower()]
    return {
        "intents":     sorted(df["Intent"].unique().tolist()),
        "total":       df["Intent"].nunique(),
        "areas":       sorted(engine.df["Area"].unique().tolist()),  # type: ignore
        "filter_area": area,
    }
