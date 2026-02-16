# 🎯 IntentRAG

A production-ready **Intent Classification** system powered by Hybrid RAG (Retrieval-Augmented Generation). Upload your training data, choose a search strategy, and classify any natural language query into its matching intent — with LLM-powered reasoning.

---

## ✨ Features

| Feature | Detail |
|---|---|
| **Hybrid Search** | BM25 (lexical) + ChromaDB (semantic vector) fusion |
| **Simple Search** | Pure semantic cosine similarity via ChromaDB |
| **File Upload** | CSV, Excel (`.xlsx`/`.xls`), or PDF |
| **LLM Reasoning** | Gemini 1.5 Flash or OpenAI GPT-4o-mini |
| **Embeddings** | Google `text-embedding-004` / OpenAI `text-embedding-3-small` |
| **Top-K Results** | Retrieve 1–10 training examples per query |
| **Streamlit UI** | Clean web dashboard with batch processing and history |
| **FastAPI Service** | REST API with Swagger docs, file upload, and batch endpoints |

---

## 🗂️ Project Structure

```
rag_intent_app/
├── core.py                  # Shared RAG engine (BM25, ChromaDB, LLM)
├── streamlit_app.py         # Streamlit web UI
├── api.py                   # FastAPI REST service
├── rag_app.py               # Original CLI application
├── test_retrieval.py        # Offline retrieval test (no API key needed)
├── requirements.txt         # Python dependencies
├── .env.example             # Environment variable template
└── README.md                # This file
```

---

## ⚙️ Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure API keys

Copy the example env file and add your key:

```bash
cp .env.example .env
```

Open `.env` and fill in your key:

```env
# Use ONE of the following:

# Google Gemini (free tier available)
# Get key at: https://aistudio.google.com/app/apikey
GOOGLE_API_KEY=your_google_api_key_here

# OpenAI GPT
# Get key at: https://platform.openai.com/api-keys
OPENAI_API_KEY=your_openai_api_key_here
```

> **Note:** The API key is read automatically from `.env` — you never need to paste it in the UI.

### 3. Prepare your training file

Your file must have at minimum these two columns:

| Column | Description | Aliases accepted |
|---|---|---|
| `Utterance` | Example user query | `query`, `question`, `text`, `input` |
| `Intent` | Intent label | `label`, `class`, `category`, `output` |
| `Area` *(optional)* | Domain / topic grouping | `domain`, `topic`, `section`, `group` |

**Supported formats:** `.csv`, `.xlsx`, `.xls`, `.pdf`

> PDF files: each non-trivial line of text becomes an utterance (Intent is left blank).

---

## 🚀 Running the App

### Option A — Streamlit UI

```bash
streamlit run streamlit_app.py
```

Opens at **http://localhost:8501**

**Sidebar walkthrough:**
1. Select your **LLM Provider** (Gemini or OpenAI) — the key status shows ✓ or ✗ automatically
2. **Upload** your training file (CSV / Excel / PDF)
3. Choose a **Search Strategy**
4. Set your **Top-K** value
5. Click **Build Index & Load Engine**

**Tabs:**

| Tab | Description |
|---|---|
| 🔍 **Single Query** | Type a query, get an intent + confidence + reasoning |
| 📦 **Batch Query** | Classify up to 50 queries at once, download results as CSV |
| 🗂️ **Explore Data** | Browse training utterances, filter by area, view distribution charts |
| 📜 **History** | Review all queries from the current session, export to CSV |

---

### Option B — FastAPI REST Service

```bash
uvicorn api:app --host 0.0.0.0 --port 8000 --reload
```

Opens at **http://localhost:8000**

**Interactive docs:** http://localhost:8000/docs

#### Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/` | Service info |
| `GET` | `/health` | Engine status, model info, loaded file |
| `POST` | `/upload` | Upload a file to build the index |
| `POST` | `/predict` | Classify a single query |
| `POST` | `/predict/batch` | Classify up to 50 queries |
| `GET` | `/intents` | List all known intents (filterable by area) |

#### Example: Upload a file

```bash
curl -X POST http://localhost:8000/upload \
  -F "file=@Intent_Training_Data.xlsx" \
  -F "provider=gemini" \
  -F "strategy=hybrid" \
  -F "alpha=0.6"
```

#### Example: Single prediction

```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"query": "user Query", "strategy": "hybrid", "top_k": 3}'
```

```json
{
  "query": "user Query",
  "intent": "MATCHED_INTENT",
  "confidence": "high",
  "reasoning": "The query directly matches the password reset training utterance.",
  "area": "IT Support",
  "strategy": "hybrid",
  "top_k": 3,
  "retrieved_utterance": "Provide me the steps to reset my password",
  "retrieved_docs": [...],
  "scores": { "hybrid": 0.9412, "semantic": 0.9621, "bm25": 0.8843 },
  "latency_ms": 342
}
```

#### Example: Batch prediction

```bash
curl -X POST http://localhost:8000/predict/batch \
  -H "Content-Type: application/json" \
  -d '{"queries": ["Query1", "Query2", "Query13"], "top_k": 1}'
```

#### Environment variables for the API

```bash
export EXCEL_PATH="Intent_data.xlsx"   # Auto-load on startup
export LLM_PROVIDER="gemini"                    # gemini | openai
export GOOGLE_API_KEY="your-key"
export HYBRID_ALPHA="0.6"
export SEARCH_STRATEGY="hybrid"                 # hybrid | simple
export CHROMA_DIR="./chroma_db"
```

---

### Option C — CLI (original)

```bash
# Interactive mode
python rag_app.py --provider gemini

# Single query
python rag_app.py --query "User Query"

# All options
python rag_app.py --excel data.xlsx --provider openai --alpha 0.5 --query "my question"
```

---

## 🔍 Search Strategies

### Hybrid (recommended)

Combines two scoring signals and fuses them with a weighted sum:

```
Final Score = α × semantic_score  +  (1 - α) × BM25_score
```

- **BM25** — term-frequency lexical matching. Excels at exact keyword queries (e.g., "RSA PIN", "AIMS request")
- **Semantic** — cosine similarity in embedding space. Handles paraphrasing (e.g., "change my pin" → `GENERATE_RSA_PIN`)
- Default **α = 0.6** (60% semantic, 40% BM25)

### Simple

Pure cosine similarity using the ChromaDB vector index. Faster, good for natural language queries with no domain jargon.

### Tuning α

| α | Behaviour |
|---|---|
| `1.0` | Pure semantic (ignores keywords) |
| `0.6` | Default — balanced, general purpose |
| `0.3` | Keywords weighted more (good for code/jargon) |
| `0.0` | Pure BM25 (fastest, exact terms only) |

---

## 🏗️ Architecture

```
                    ┌──────────────┐     ┌──────────────────┐
                    │ Streamlit UI │     │  FastAPI Service  │
                    │  :8501       │     │  :8000           │
                    └──────┬───────┘     └────────┬─────────┘
                           │                      │
                           └──────────┬───────────┘
                                      ▼
                          ┌───────────────────────┐
                          │       core.py          │
                          │   IntentRAGEngine      │
                          │                       │
                          │  ┌────────┐ ┌───────┐ │
                          │  │  BM25  │ │Chroma │ │
                          │  │lexical │ │vector │ │
                          │  └───┬────┘ └───┬───┘ │
                          │      └─────┬────┘     │
                          │      Hybrid Fusion     │
                          │       (alpha=0.6)      │
                          │            │           │
                          │     Top-K Results      │
                          │            │           │
                          │  LangChain + LLM       │
                          │ (Gemini / OpenAI)      │
                          └───────────────────────┘
                                      │
                          Intent · Confidence · Reasoning
```

---

## 🔑 API Key Reference

| Provider | Key Name | Model (LLM) | Model (Embeddings) |
|---|---|---|---|
| Google | `GOOGLE_API_KEY` | `gemini-1.5-flash` | `text-embedding-004` |
| OpenAI | `OPENAI_API_KEY` | `gpt-4o-mini` | `text-embedding-3-small` |

One key covers both the LLM calls and the embedding calls. No open-source or local models are used.

---

## 🧪 Testing Retrieval (no API key needed)

Run the offline retrieval test to verify ChromaDB + BM25 are working before connecting an LLM:

```bash
python test_retrieval.py
```

This builds the index and runs sample queries, printing matched intents and scores — without making any LLM API calls.

---

## 📦 Dependencies

| Package | Purpose |
|---|---|
| `langchain` | LLM chain orchestration |
| `langchain-google-genai` | Gemini Flash integration |
| `langchain-openai` | OpenAI GPT integration |
| `chromadb` | Vector store (HNSW cosine index) |
| `google-generativeai` | Google embedding API |
| `openai` | OpenAI embedding API |
| `pandas` + `openpyxl` | CSV / Excel file parsing |
| `pdfplumber` | PDF text extraction |
| `fastapi` + `uvicorn` | REST API server |
| `streamlit` | Web dashboard UI |
| `python-dotenv` | `.env` file loading |

---

## 📄 License

MIT
