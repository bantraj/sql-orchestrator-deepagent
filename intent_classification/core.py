"""
core.py  –  Enhanced Shared RAG Engine
========================================
• File ingestion  : CSV, Excel (.xlsx/.xls), PDF
• Search strategy : "simple" (semantic-only) or "hybrid" (BM25 + semantic)
• Top-K           : configurable at query time
• LLM             : Gemini 1.5 Flash  |  OpenAI GPT-4o-mini
• Embeddings      : Google text-embedding-004  |  OpenAI text-embedding-3-small
  (NO open-source / HuggingFace models — API keys required for both LLM + embeddings)
"""

from __future__ import annotations

import io
import json
import math
import os
import re
from collections import Counter
from pathlib import Path
from typing import Literal, Optional

import pandas as pd

SearchStrategy = Literal["simple", "hybrid"]

# ─────────────────────────────────────────────────────────────────────────────
# 1. BM25  (pure-Python, zero external deps)
# ─────────────────────────────────────────────────────────────────────────────

class BM25:
    def __init__(self, k1: float = 1.5, b: float = 0.75):
        self.k1, self.b = k1, b
        self.corpus:    list[list[str]] = []
        self.doc_freqs: list[Counter]   = []
        self.idf:       dict[str, float] = {}
        self.avgdl:     float = 0.0
        self._n:        int   = 0

    def _tokenize(self, text: str) -> list[str]:
        return re.sub(r"[^\w\s]", " ", text.lower()).split()

    def fit(self, docs: list[str]) -> None:
        self.corpus    = [self._tokenize(d) for d in docs]
        self._n        = len(self.corpus)
        self.doc_freqs = [Counter(c) for c in self.corpus]
        self.avgdl     = sum(len(c) for c in self.corpus) / max(1, self._n)
        df: Counter    = Counter()
        for c in self.corpus:
            df.update(set(c))
        self.idf = {
            t: math.log((self._n - f + 0.5) / (f + 0.5) + 1)
            for t, f in df.items()
        }

    def score(self, query: str) -> list[float]:
        tokens = self._tokenize(query)
        scores: list[float] = []
        for doc, tf in zip(self.corpus, self.doc_freqs):
            dl, s = len(doc), 0.0
            for t in tokens:
                if t not in self.idf:
                    continue
                freq = tf[t]
                num  = self.idf[t] * freq * (self.k1 + 1)
                den  = freq + self.k1 * (1 - self.b + self.b * dl / max(1, self.avgdl))
                s   += num / den
            scores.append(s)
        return scores


# ─────────────────────────────────────────────────────────────────────────────
# 2. File ingestion  –  CSV / Excel / PDF → pd.DataFrame
# ─────────────────────────────────────────────────────────────────────────────

def _read_pdf(file_bytes: bytes) -> pd.DataFrame:
    try:
        import pdfplumber
    except ImportError:
        raise ImportError("Install pdfplumber:  pip install pdfplumber")

    lines: list[str] = []
    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            for line in text.splitlines():
                line = line.strip()
                if len(line) > 10:
                    lines.append(line)

    if not lines:
        raise ValueError("No readable text found in the PDF.")

    df = pd.DataFrame({"Utterance": lines})
    df["Intent"] = ""
    df["Area"]   = "PDF Import"
    return df


def load_file(source, file_name: str = "") -> pd.DataFrame:
    """
    Universal file loader.
    Accepts: file path (str/Path), raw bytes, BytesIO, or Streamlit UploadedFile.
    Returns DataFrame with columns: Utterance, Intent, Area (always).
    """
    if isinstance(source, (bytes, bytearray)):
        raw   = bytes(source)
        fname = file_name.lower()
    elif hasattr(source, "read"):
        raw   = source.read()
        fname = (getattr(source, "name", file_name) or file_name).lower()
    else:
        path  = Path(source)
        raw   = path.read_bytes()
        fname = path.name.lower()

    if fname.endswith(".pdf"):
        df = _read_pdf(raw)
    elif fname.endswith(".csv"):
        df = pd.read_csv(io.BytesIO(raw))
    elif fname.endswith((".xlsx", ".xls")):
        df = pd.read_excel(io.BytesIO(raw))
    else:
        raise ValueError(f"Unsupported file: '{fname}'. Accepted: .csv .xlsx .xls .pdf")

    # Normalise column names via aliases
    col_aliases = {
        "Utterance": ["utterance", "query", "question", "text", "sentence", "input"],
        "Intent":    ["intent",    "label", "class",    "category", "output"],
        "Area":      ["area",      "domain","topic",    "section",  "group"],
    }
    lower_cols = {c.lower(): c for c in df.columns}
    rename_map: dict[str, str] = {}
    for canonical, aliases in col_aliases.items():
        if canonical not in df.columns:
            for alias in aliases:
                if alias in lower_cols:
                    rename_map[lower_cols[alias]] = canonical
                    break
    if rename_map:
        df = df.rename(columns=rename_map)

    if "Utterance" not in df.columns:
        text_cols = df.select_dtypes(include="object").columns.tolist()
        if text_cols:
            df = df.rename(columns={text_cols[0]: "Utterance"})
        else:
            raise ValueError("Cannot find a text column to use as 'Utterance'.")

    for col in ["Intent", "Area"]:
        if col not in df.columns:
            df[col] = ""

    df = df.dropna(subset=["Utterance"])
    df["Utterance"] = df["Utterance"].astype(str).str.strip()
    df["Intent"]    = df["Intent"].astype(str).str.strip()
    df["Area"]      = df["Area"].astype(str).str.strip()
    df = df[df["Utterance"].str.len() > 2].reset_index(drop=True)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# 3. Commercial Embedding Functions
#    • Google  : text-embedding-004   (via GOOGLE_API_KEY)
#    • OpenAI  : text-embedding-3-small (via OPENAI_API_KEY)
#    NO open-source / HuggingFace / local models used anywhere.
# ─────────────────────────────────────────────────────────────────────────────

class _GoogleEmbeddingFunction:
    """
    Wraps google-generativeai embeddings (text-embedding-004) for ChromaDB.
    Implements the ChromaDB EmbeddingFunction protocol.
    """

    def __init__(self, api_key: str):
        try:
            import google.generativeai as genai
        except ImportError:
            raise ImportError("Run: pip install google-generativeai")
        genai.configure(api_key=api_key)
        self._genai = genai
        self.model  = "models/text-embedding-004"

    def __call__(self, input: list[str]) -> list[list[float]]:   # noqa: A002
        embeddings = []
        # Google API batches up to 100 texts at a time
        batch_size = 100
        for i in range(0, len(input), batch_size):
            batch = input[i : i + batch_size]
            result = self._genai.embed_content(
                model=self.model,
                content=batch,
                task_type="retrieval_document",
            )
            embeddings.extend(result["embedding"])
        return embeddings


class _OpenAIEmbeddingFunction:
    """
    Wraps OpenAI embeddings (text-embedding-3-small) for ChromaDB.
    Implements the ChromaDB EmbeddingFunction protocol.
    """

    def __init__(self, api_key: str):
        try:
            import openai
        except ImportError:
            raise ImportError("Run: pip install openai")
        self._client = openai.OpenAI(api_key=api_key)
        self.model   = "text-embedding-3-small"

    def __call__(self, input: list[str]) -> list[list[float]]:   # noqa: A002
        # OpenAI supports up to 2048 inputs per call; batch 500 for safety
        embeddings = []
        batch_size = 500
        for i in range(0, len(input), batch_size):
            batch = input[i : i + batch_size]
            response = self._client.embeddings.create(
                model=self.model,
                input=batch,
            )
            embeddings.extend([item.embedding for item in response.data])
        return embeddings


def _get_embedding_function(provider: str, api_key: str):
    """
    Return the correct commercial embedding function for the given provider.
    Raises if the API key is missing.
    """
    if not api_key:
        raise EnvironmentError(
            f"API key required for '{provider}' embeddings. "
            "Set GOOGLE_API_KEY or OPENAI_API_KEY."
        )
    if provider == "gemini":
        return _GoogleEmbeddingFunction(api_key=api_key)
    elif provider == "openai":
        return _OpenAIEmbeddingFunction(api_key=api_key)
    else:
        raise ValueError(f"Unknown embedding provider '{provider}'. Use 'gemini' or 'openai'.")


# ─────────────────────────────────────────────────────────────────────────────
# 4. ChromaDB vector store builder
# ─────────────────────────────────────────────────────────────────────────────

def build_chroma_store(
    df:           pd.DataFrame,
    provider:     str,
    api_key:      str,
    persist_dir:  str = "./chroma_db",
):
    """
    Build a ChromaDB collection using commercial embeddings only.

    provider : "gemini"  → Google text-embedding-004
               "openai"  → OpenAI text-embedding-3-small
    """
    import chromadb

    ef     = _get_embedding_function(provider, api_key)
    client = chromadb.PersistentClient(path=persist_dir)

    col_name = "intent_store"
    try:
        client.delete_collection(col_name)
    except Exception:
        pass

    collection = client.create_collection(
        name=col_name,
        embedding_function=ef,
        metadata={"hnsw:space": "cosine"},
    )
    collection.add(
        documents=df["Utterance"].tolist(),
        metadatas=[
            {"intent": i, "area": a}
            for i, a in zip(df["Intent"].tolist(), df["Area"].tolist())
        ],
        ids=[f"doc_{idx}" for idx in range(len(df))],
    )
    return collection


# ─────────────────────────────────────────────────────────────────────────────
# 5. Retriever  –  simple (semantic) OR hybrid (BM25 + semantic)
# ─────────────────────────────────────────────────────────────────────────────

class Retriever:
    def __init__(
        self,
        df: pd.DataFrame,
        collection,
        strategy: SearchStrategy = "hybrid",
        alpha: float = 0.6,
    ):
        self.df         = df.reset_index(drop=True)
        self.collection = collection
        self.strategy   = strategy
        self.alpha      = alpha
        self.utterances = df["Utterance"].tolist()
        self.intents    = df["Intent"].tolist()
        self.areas      = df["Area"].tolist() if "Area" in df.columns else [""] * len(df)
        self.bm25 = BM25()
        self.bm25.fit(self.utterances)

    @staticmethod
    def _normalize(scores: list[float]) -> list[float]:
        mn, mx = min(scores), max(scores)
        if mx == mn:
            return [1.0] * len(scores)
        return [(s - mn) / (mx - mn) for s in scores]

    def search(self, query: str, top_k: int = 1) -> list[dict]:
        n     = len(self.utterances)
        top_k = max(1, min(top_k, n))

        chroma_k = min(n, max(top_k + 5, 10))
        results  = self.collection.query(
            query_texts=[query], n_results=chroma_k,
            include=["distances", "metadatas", "documents"],
        )
        sem_scores = [0.0] * n
        for doc_id, dist in zip(results["ids"][0], results["distances"][0]):
            idx = int(doc_id.split("_")[1])
            sem_scores[idx] = max(0.0, 1.0 - dist)

        if self.strategy == "simple":
            norm_sem = self._normalize(sem_scores)
            ranked   = sorted(range(n), key=lambda i: norm_sem[i], reverse=True)[:top_k]
            return [
                {
                    "rank": r + 1, "utterance": self.utterances[i],
                    "intent": self.intents[i], "area": self.areas[i],
                    "strategy": "simple",
                    "hybrid_score": None,
                    "semantic_score": round(norm_sem[i], 4),
                    "bm25_score": None,
                }
                for r, i in enumerate(ranked)
            ]
        else:
            norm_bm25 = self._normalize(self.bm25.score(query))
            norm_sem  = self._normalize(sem_scores)
            hybrid    = [
                self.alpha * s + (1 - self.alpha) * b
                for s, b in zip(norm_sem, norm_bm25)
            ]
            ranked = sorted(range(n), key=lambda i: hybrid[i], reverse=True)[:top_k]
            return [
                {
                    "rank": r + 1, "utterance": self.utterances[i],
                    "intent": self.intents[i], "area": self.areas[i],
                    "strategy": "hybrid",
                    "hybrid_score":   round(hybrid[i], 4),
                    "semantic_score": round(norm_sem[i], 4),
                    "bm25_score":     round(norm_bm25[i], 4),
                }
                for r, i in enumerate(ranked)
            ]


# ─────────────────────────────────────────────────────────────────────────────
# 6. LLM factory
# ─────────────────────────────────────────────────────────────────────────────

def get_llm(provider: str, api_key: Optional[str] = None):
    if provider == "gemini":
        from langchain_google_genai import ChatGoogleGenerativeAI
        key = api_key or os.environ.get("GOOGLE_API_KEY", "")
        if not key:
            raise EnvironmentError(
                "GOOGLE_API_KEY not set. "
                "Get a free key at https://aistudio.google.com/app/apikey"
            )
        return ChatGoogleGenerativeAI(
            model="gemini-1.5-flash", google_api_key=key, temperature=0
        )
    elif provider == "openai":
        from langchain_openai import ChatOpenAI
        key = api_key or os.environ.get("OPENAI_API_KEY", "")
        if not key:
            raise EnvironmentError("OPENAI_API_KEY not set.")
        return ChatOpenAI(model="gpt-4o-mini", openai_api_key=key, temperature=0)
    else:
        raise ValueError(f"Unknown provider '{provider}'.")


# ─────────────────────────────────────────────────────────────────────────────
# 7. Prompts
# ─────────────────────────────────────────────────────────────────────────────

PROMPT_SINGLE = """\
You are an intent classification assistant.
A single training example was retrieved for the user query.

Retrieved Example:
  Utterance : {utterance}
  Intent    : {intent}
  Area      : {area}

User Query: "{user_query}"

Respond ONLY with a JSON object (no markdown fences):
{{
  "intent": "<INTENT_NAME>",
  "confidence": "<high|medium|low>",
  "reasoning": "<one concise sentence>"
}}"""

PROMPT_MULTI = """\
You are an intent classification assistant.
The top-{k} retrieved training examples are listed below (ranked by relevance).

{examples}

User Query: "{user_query}"

Analyse ALL examples, then choose the BEST matching intent.
Respond ONLY with a JSON object (no markdown fences):
{{
  "intent": "<INTENT_NAME>",
  "confidence": "<high|medium|low>",
  "reasoning": "<one concise sentence explaining your choice>"
}}"""


def _build_prompt(query: str, hits: list[dict]) -> str:
    if len(hits) == 1:
        h = hits[0]
        return PROMPT_SINGLE.format(
            utterance=h["utterance"], intent=h["intent"],
            area=h["area"], user_query=query,
        )
    examples = "\n".join(
        f"  [{h['rank']}] Utterance: \"{h['utterance']}\"\n"
        f"       Intent   : {h['intent'] or '(unknown)'}\n"
        f"       Area     : {h['area']   or '—'}"
        for h in hits
    )
    return PROMPT_MULTI.format(k=len(hits), examples=examples, user_query=query)


def _parse_llm_json(raw: str, fallback: str) -> dict:
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    try:
        return json.loads(raw.strip())
    except json.JSONDecodeError:
        return {"intent": fallback, "confidence": "medium",
                "reasoning": "Direct retrieval match (LLM parse error)."}


# ─────────────────────────────────────────────────────────────────────────────
# 8. Main RAG engine
# ─────────────────────────────────────────────────────────────────────────────

class IntentRAGEngine:
    """
    Single shared engine used by FastAPI and Streamlit.

    Build:  engine = IntentRAGEngine.from_file(source, ...)
    Query:  result = engine.predict(query, strategy="hybrid", top_k=3)
    """

    def __init__(
        self,
        df:           pd.DataFrame,
        llm_provider: str              = "gemini",
        api_key:      Optional[str]    = None,
        alpha:        float            = 0.6,
        persist_dir:  str              = "./chroma_db",
        strategy:     SearchStrategy   = "hybrid",
    ):
        self.df         = df
        self.provider   = llm_provider
        self.alpha      = alpha
        self.strategy   = strategy
        self.total_docs = len(df)

        # Resolve key: explicit arg beats environment variable
        resolved_key = api_key or (
            os.environ.get("GOOGLE_API_KEY", "") if llm_provider == "gemini"
            else os.environ.get("OPENAI_API_KEY", "")
        )

        # Embeddings use the SAME provider + key as the LLM (no open-source models)
        collection     = build_chroma_store(
            df=df, provider=llm_provider,
            api_key=resolved_key, persist_dir=persist_dir,
        )
        self.retriever = Retriever(df, collection, strategy=strategy, alpha=alpha)
        self.llm       = get_llm(llm_provider, resolved_key)

        self.intent_list = sorted(df["Intent"].unique().tolist())
        self.area_list   = (
            sorted(df["Area"].dropna().unique().tolist())
            if "Area" in df.columns else []
        )

    @classmethod
    def from_file(
        cls,
        source,
        file_name:    str            = "",
        llm_provider: str            = "gemini",
        api_key:      Optional[str]  = None,
        alpha:        float          = 0.6,
        persist_dir:  str            = "./chroma_db",
        strategy:     SearchStrategy = "hybrid",
    ) -> "IntentRAGEngine":
        df = load_file(source, file_name)
        return cls(
            df=df, llm_provider=llm_provider, api_key=api_key,
            alpha=alpha, persist_dir=persist_dir, strategy=strategy,
        )

    def predict(
        self,
        query:    str,
        strategy: Optional[SearchStrategy] = None,
        top_k:    int = 1,
    ) -> dict:
        active = strategy or self.strategy
        self.retriever.strategy = active

        hits   = self.retriever.search(query, top_k=top_k)
        prompt = _build_prompt(query, hits)

        from langchain_core.messages import HumanMessage
        response = self.llm.invoke([HumanMessage(content=prompt)])
        parsed   = _parse_llm_json(response.content, hits[0]["intent"])

        return {
            "query":      query,
            "intent":     parsed.get("intent",     hits[0]["intent"]),
            "confidence": parsed.get("confidence", "medium"),
            "reasoning":  parsed.get("reasoning",  ""),
            "strategy":   active,
            "top_k":      top_k,
            "retrieved":  hits,
            # top-1 convenience aliases (backward compat)
            "retrieved_utterance": hits[0]["utterance"],
            "area":                hits[0]["area"],
            "hybrid_score":        hits[0].get("hybrid_score"),
            "semantic_score":      hits[0].get("semantic_score"),
            "bm25_score":          hits[0].get("bm25_score"),
        }
