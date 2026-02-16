"""
Intent RAG Application
======================
Hybrid RAG (BM25 + ChromaDB Vector Search) using LangChain
Supports: Gemini Flash (default) or OpenAI GPT
"""

import os
import json
import math
import pickle
from pathlib import Path
from typing import Optional

import pandas as pd
from collections import Counter


# ── 1. BM25 Implementation (no extra dependency needed) ──────────────────────

class BM25:
    """Lightweight BM25 implementation (no rank_bm25 needed)."""

    def __init__(self, k1: float = 1.5, b: float = 0.75):
        self.k1 = k1
        self.b = b
        self.corpus: list[list[str]] = []
        self.doc_freqs: list[Counter] = []
        self.idf: dict[str, float] = {}
        self.avgdl: float = 0.0
        self._n: int = 0

    def _tokenize(self, text: str) -> list[str]:
        return text.lower().split()

    def fit(self, docs: list[str]) -> None:
        self.corpus = [self._tokenize(d) for d in docs]
        self._n = len(self.corpus)
        self.doc_freqs = [Counter(c) for c in self.corpus]
        self.avgdl = sum(len(c) for c in self.corpus) / max(1, self._n)

        df: Counter = Counter()
        for c in self.corpus:
            df.update(set(c))

        self.idf = {}
        for term, freq in df.items():
            self.idf[term] = math.log(
                (self._n - freq + 0.5) / (freq + 0.5) + 1
            )

    def score(self, query: str) -> list[float]:
        tokens = self._tokenize(query)
        scores = []
        for i, (doc, tf) in enumerate(zip(self.corpus, self.doc_freqs)):
            dl = len(doc)
            s = 0.0
            for t in tokens:
                if t not in self.idf:
                    continue
                freq = tf[t]
                num = self.idf[t] * freq * (self.k1 + 1)
                den = freq + self.k1 * (
                    1 - self.b + self.b * dl / max(1, self.avgdl)
                )
                s += num / den
            scores.append(s)
        return scores


# ── 2. Data Loading ───────────────────────────────────────────────────────────

def load_data(excel_path: str) -> pd.DataFrame:
    df = pd.read_excel(excel_path)
    required = {"Utterance", "Intent"}
    if not required.issubset(df.columns):
        raise ValueError(f"Excel must have columns: {required}. Found: {set(df.columns)}")
    df = df.dropna(subset=["Utterance", "Intent"])
    df["Utterance"] = df["Utterance"].astype(str).str.strip()
    df["Intent"]    = df["Intent"].astype(str).str.strip()
    print(f"✅ Loaded {len(df)} intent records from '{excel_path}'")
    return df


# ── 3. ChromaDB Vector Store ──────────────────────────────────────────────────

def build_chroma_store(df: pd.DataFrame, provider: str, api_key: str, persist_dir: str = "./chroma_db"):
    """Build ChromaDB collection using commercial embeddings only (Google or OpenAI)."""
    try:
        import chromadb
    except ImportError:
        raise ImportError("Run: pip install chromadb")

    client = chromadb.PersistentClient(path=persist_dir)

    # ── Commercial embedding function (no open-source / HuggingFace models) ──
    if provider == "gemini":
        import google.generativeai as genai
        genai.configure(api_key=api_key)

        class _GoogleEF:
            def __call__(self, input):   # noqa: A002
                result = genai.embed_content(
                    model="models/text-embedding-004",
                    content=input,
                    task_type="retrieval_document",
                )
                return result["embedding"]

        ef = _GoogleEF()
        print("✅ Using Google text-embedding-004")

    elif provider == "openai":
        import openai as _openai
        _oa_client = _openai.OpenAI(api_key=api_key)

        class _OpenAIEF:
            def __call__(self, input):   # noqa: A002
                resp = _oa_client.embeddings.create(
                    model="text-embedding-3-small", input=input
                )
                return [item.embedding for item in resp.data]

        ef = _OpenAIEF()
        print("✅ Using OpenAI text-embedding-3-small")

    else:
        raise ValueError(f"Unknown provider '{provider}'. Use 'gemini' or 'openai'.")

    col_name = "intent_store"

    # Delete and recreate for fresh indexing
    try:
        client.delete_collection(col_name)
    except Exception:
        pass

    collection = client.create_collection(
        name=col_name,
        embedding_function=ef,
        metadata={"hnsw:space": "cosine"},
    )

    utterances = df["Utterance"].tolist()
    intents    = df["Intent"].tolist()
    areas      = df.get("Area", [""] * len(df)).fillna("").tolist()

    collection.add(
        documents=utterances,
        metadatas=[{"intent": i, "area": a} for i, a in zip(intents, areas)],
        ids=[f"doc_{idx}" for idx in range(len(utterances))],
    )

    print(f"✅ ChromaDB collection '{col_name}' built with {len(utterances)} docs")
    return collection


# ── 4. Hybrid Retriever ───────────────────────────────────────────────────────

class HybridRetriever:
    """
    Hybrid search: BM25 (lexical) + ChromaDB (semantic)
    Final score = alpha * semantic_score + (1 - alpha) * bm25_score
    """

    def __init__(self, df: pd.DataFrame, collection, alpha: float = 0.6):
        self.df = df.reset_index(drop=True)
        self.collection = collection
        self.alpha = alpha  # weight for semantic

        self.utterances = df["Utterance"].tolist()
        self.intents    = df["Intent"].tolist()
        self.areas      = df.get("Area", pd.Series([""] * len(df))).fillna("").tolist()

        # Fit BM25
        self.bm25 = BM25()
        self.bm25.fit(self.utterances)
        print("✅ BM25 index built")

    def _normalize(self, scores: list[float]) -> list[float]:
        mn, mx = min(scores), max(scores)
        if mx == mn:
            return [0.5] * len(scores)
        return [(s - mn) / (mx - mn) for s in scores]

    def search(self, query: str, top_k: int = 1) -> list[dict]:
        n = len(self.utterances)

        # ── BM25 scores ──
        raw_bm25 = self.bm25.score(query)
        norm_bm25 = self._normalize(raw_bm25)

        # ── Semantic scores via ChromaDB ──
        results = self.collection.query(
            query_texts=[query],
            n_results=min(n, 10),
            include=["distances", "metadatas", "documents"],
        )

        # Map doc id → semantic score
        sem_scores = [0.0] * n
        ids = results["ids"][0]
        distances = results["distances"][0]

        for doc_id, dist in zip(ids, distances):
            idx = int(doc_id.split("_")[1])
            # cosine distance → similarity
            sem_scores[idx] = 1.0 - dist

        norm_sem = self._normalize(sem_scores)

        # ── Hybrid fusion ──
        hybrid = [
            self.alpha * s + (1 - self.alpha) * b
            for s, b in zip(norm_sem, norm_bm25)
        ]

        # Get top-k indices
        ranked = sorted(range(n), key=lambda i: hybrid[i], reverse=True)[:top_k]

        return [
            {
                "utterance":      self.utterances[i],
                "intent":         self.intents[i],
                "area":           self.areas[i],
                "hybrid_score":   round(hybrid[i], 4),
                "semantic_score": round(norm_sem[i], 4),
                "bm25_score":     round(norm_bm25[i], 4),
            }
            for i in ranked
        ]


# ── 5. LLM Setup ─────────────────────────────────────────────────────────────

def get_llm(provider: str = "gemini"):
    """
    provider: 'gemini' | 'openai'
    Reads API key from environment variable:
      - GOOGLE_API_KEY  (for gemini)
      - OPENAI_API_KEY  (for openai)
    """
    if provider == "gemini":
        try:
            from langchain_google_genai import ChatGoogleGenerativeAI
        except ImportError:
            raise ImportError("Run: pip install langchain-google-genai")

        api_key = os.environ.get("GOOGLE_API_KEY", "")
        if not api_key:
            raise EnvironmentError(
                "Set GOOGLE_API_KEY environment variable.\n"
                "  export GOOGLE_API_KEY='your-key-here'"
            )
        llm = ChatGoogleGenerativeAI(
            model="gemini-1.5-flash",
            google_api_key=api_key,
            temperature=0,
        )
        print("✅ Using Gemini 1.5 Flash LLM")
        return llm

    elif provider == "openai":
        try:
            from langchain_openai import ChatOpenAI
        except ImportError:
            raise ImportError("Run: pip install langchain-openai")

        api_key = os.environ.get("OPENAI_API_KEY", "")
        if not api_key:
            raise EnvironmentError(
                "Set OPENAI_API_KEY environment variable.\n"
                "  export OPENAI_API_KEY='your-key-here'"
            )
        llm = ChatOpenAI(
            model="gpt-4o-mini",
            openai_api_key=api_key,
            temperature=0,
        )
        print("✅ Using OpenAI GPT-4o-mini LLM")
        return llm

    else:
        raise ValueError(f"Unknown provider '{provider}'. Use 'gemini' or 'openai'.")


# ── 6. RAG Chain ──────────────────────────────────────────────────────────────

PROMPT_TEMPLATE = """You are an intent classification assistant. 
Based on the retrieved training example below, determine the correct intent for the user's query.

Retrieved Example:
  Utterance : {retrieved_utterance}
  Intent    : {retrieved_intent}
  Area      : {retrieved_area}

User Query: "{user_query}"

Respond ONLY with a JSON object in this exact format:
{{
  "intent": "<INTENT_NAME>",
  "confidence": "<high|medium|low>",
  "reasoning": "<one sentence explanation>"
}}"""


class IntentRAGApp:
    def __init__(
        self,
        excel_path:   str,
        llm_provider: str = "gemini",
        api_key:      str = "",
        alpha:        float = 0.6,
        persist_dir:  str = "./chroma_db",
    ):
        print("\n🔧 Initializing Intent RAG Application...\n")
        resolved_key = api_key or (
            os.environ.get("GOOGLE_API_KEY", "") if llm_provider == "gemini"
            else os.environ.get("OPENAI_API_KEY", "")
        )
        df = load_data(excel_path)
        # Embeddings use the same commercial provider as the LLM (no open-source models)
        collection = build_chroma_store(df, provider=llm_provider,
                                        api_key=resolved_key, persist_dir=persist_dir)
        self.retriever = HybridRetriever(df, collection, alpha=alpha)
        self.llm = get_llm(llm_provider)
        print("\n✅ RAG Application ready!\n")

    def predict(self, query: str, verbose: bool = False) -> dict:
        """
        Search utterances and return top-1 intent with LLM reasoning.
        """
        # Step 1: Hybrid retrieval
        hits = self.retriever.search(query, top_k=1)
        top = hits[0]

        if verbose:
            print(f"\n📎 Retrieved: '{top['utterance']}'")
            print(f"   Intent   : {top['intent']}")
            print(f"   Scores   : hybrid={top['hybrid_score']}  "
                  f"sem={top['semantic_score']}  bm25={top['bm25_score']}")

        # Step 2: LLM reasoning
        prompt = PROMPT_TEMPLATE.format(
            retrieved_utterance=top["utterance"],
            retrieved_intent=top["intent"],
            retrieved_area=top["area"],
            user_query=query,
        )

        from langchain_core.messages import HumanMessage
        response = self.llm.invoke([HumanMessage(content=prompt)])
        raw = response.content.strip()

        # Parse JSON from LLM
        try:
            # Strip markdown code fences if present
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            result = json.loads(raw.strip())
        except json.JSONDecodeError:
            # Fallback: return retrieved intent directly
            result = {
                "intent":     top["intent"],
                "confidence": "high",
                "reasoning":  "Direct match from retrieval.",
            }

        return {
            "query":              query,
            "intent":             result.get("intent", top["intent"]),
            "confidence":         result.get("confidence", "medium"),
            "reasoning":          result.get("reasoning", ""),
            "retrieved_utterance": top["utterance"],
            "area":               top["area"],
            "hybrid_score":       top["hybrid_score"],
        }


# ── 7. CLI Entry Point ────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Intent RAG Application")
    parser.add_argument("--excel",    default="Intent_Training_Data.xlsx",
                        help="Path to the Excel training data file")
    parser.add_argument("--provider", default="gemini",
                        choices=["gemini", "openai"],
                        help="LLM provider: 'gemini' (default) or 'openai'")
    parser.add_argument("--alpha",    type=float, default=0.6,
                        help="Semantic weight (0-1). Default 0.6")
    parser.add_argument("--query",    type=str, default=None,
                        help="Single query to run (non-interactive mode)")
    args = parser.parse_args()

    app = IntentRAGApp(
        excel_path=args.excel,
        llm_provider=args.provider,
        alpha=args.alpha,
    )

    if args.query:
        result = app.predict(args.query, verbose=True)
        print("\n" + "═" * 50)
        print(f"  Query     : {result['query']}")
        print(f"  Intent    : {result['intent']}")
        print(f"  Confidence: {result['confidence']}")
        print(f"  Area      : {result['area']}")
        print(f"  Reasoning : {result['reasoning']}")
        print("═" * 50)
        return

    # Interactive mode
    print("=" * 60)
    print("  Intent RAG App  |  Type 'quit' to exit")
    print("=" * 60)
    while True:
        query = input("\n🔍 Enter your query: ").strip()
        if query.lower() in {"quit", "exit", "q"}:
            print("Goodbye!")
            break
        if not query:
            continue

        result = app.predict(query, verbose=True)
        print("\n" + "─" * 50)
        print(f"  🎯 Intent    : {result['intent']}")
        print(f"  📊 Confidence: {result['confidence']}")
        print(f"  🏢 Area      : {result['area']}")
        print(f"  💬 Reasoning : {result['reasoning']}")
        print(f"  🔗 Best Match: \"{result['retrieved_utterance']}\"")
        print(f"  📈 Score     : {result['hybrid_score']}")
        print("─" * 50)


if __name__ == "__main__":
    main()
