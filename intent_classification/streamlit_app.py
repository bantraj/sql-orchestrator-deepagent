"""
streamlit_app.py  –  Intent RAG UI  (v3)
=========================================
Changes vs v2:
  • API key read from .env file (GOOGLE_API_KEY / OPENAI_API_KEY) — no UI input
  • Index Stats panel removed from sidebar
  • Result card uses clean Streamlit-native styling — no dark theme
  • Score Breakdown section removed
  • Retrieved Documents use clean expanders — no dark card theme
  • Dark CSS overrides removed for inputs, radio, result card, doc cards
Run:
  streamlit run streamlit_app.py
"""

from __future__ import annotations

import os
import time
from pathlib import Path

import pandas as pd
import streamlit as st

# ── Load .env BEFORE anything else ───────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env", override=False)
except ImportError:
    pass   # python-dotenv not installed — env vars must be set externally

# ── Page config (must be FIRST Streamlit call) ────────────────────────────────
st.set_page_config(
    page_title="Intent RAG",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS  ──────────────────────────────────────────────────────────────────────
# Minimal overrides only — no dark theme forcing on native widgets
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Outfit:wght@400;600;700;900&display=swap');

/* Typography */
html, body, [class*="css"] { font-family: 'Outfit', sans-serif !important; }

/* Layout */
.block-container { padding: 1.4rem 2rem 2rem !important; max-width: 1300px !important; }
#MainMenu, footer { visibility: hidden; }

/* ── Branded logo in sidebar ── */
.logo-title {
    font-size: 1.7rem; font-weight: 900; letter-spacing: -.03em;
    background: linear-gradient(135deg, #0ea5e9, #8b5cf6);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    background-clip: text; margin-bottom: .1rem;
}
.logo-sub { font-size: .8rem; color: #6b7280; }

/* ── Section labels ── */
.section-lbl {
    font-size: .7rem; font-weight: 700; color: #9ca3af;
    text-transform: uppercase; letter-spacing: .1em; margin-bottom: .4rem;
}

/* ── Classify button — teal accent ── */
[data-testid="stButton"] > button[kind="primary"] {
    background: linear-gradient(135deg, #0ea5e9, #6366f1) !important;
    color: #fff !important; font-weight: 700 !important;
    border: none !important; border-radius: 8px !important;
    padding: .55rem 1.8rem !important; font-size: .9rem !important;
    letter-spacing: .02em !important; transition: all .2s !important;
}
[data-testid="stButton"] > button[kind="primary"]:hover {
    opacity: .9 !important; transform: translateY(-1px) !important;
    box-shadow: 0 4px 14px rgba(99,102,241,.35) !important;
}

/* ── Result card  (light-neutral, works in both themes) ── */
.result-card {
    border: 1.5px solid #e5e7eb;
    border-left: 5px solid #0ea5e9;
    border-radius: 12px;
    padding: 1.2rem 1.5rem;
    margin: .8rem 0;
    background: #f9fafb;
}
.result-intent {
    font-family: 'JetBrains Mono', monospace;
    font-size: 1.45rem; font-weight: 700;
    color: #0ea5e9; letter-spacing: .03em;
}
.result-meta {
    font-size: .8rem; color: #6b7280;
    margin-top: .15rem; letter-spacing: .05em;
    text-transform: uppercase;
}
.result-reasoning {
    margin-top: .7rem; padding-top: .6rem;
    border-top: 1px solid #e5e7eb;
    color: #374151; font-size: .9rem; line-height: 1.65;
}

/* ── Confidence badge ── */
.badge {
    display: inline-block; padding: .15rem .55rem; border-radius: 999px;
    font-size: .68rem; font-weight: 700; text-transform: uppercase;
    letter-spacing: .06em; margin-left: .5rem; vertical-align: middle;
}
.badge-high   { background: #dcfce7; color: #16a34a; border: 1px solid #bbf7d0; }
.badge-medium { background: #fef9c3; color: #ca8a04; border: 1px solid #fde047; }
.badge-low    { background: #fee2e2; color: #dc2626; border: 1px solid #fca5a5; }

/* ── Strategy badge ── */
.strat-badge {
    display: inline-block; padding: .15rem .55rem; border-radius: 999px;
    font-size: .68rem; font-weight: 700; text-transform: uppercase;
    letter-spacing: .06em; margin-left: .4rem; vertical-align: middle;
}
.strat-hybrid { background: #ede9fe; color: #7c3aed; border: 1px solid #ddd6fe; }
.strat-simple { background: #e0f2fe; color: #0284c7; border: 1px solid #bae6fd; }

/* ── Retrieved doc row (clean, no dark bg) ── */
.doc-row {
    border: 1px solid #e5e7eb; border-radius: 8px;
    padding: .7rem 1rem; margin: .35rem 0;
    background: #ffffff;
}
.doc-row-rank {
    display: inline-flex; align-items: center; justify-content: center;
    width: 20px; height: 20px; border-radius: 50%;
    background: #6366f1; color: #fff;
    font-size: .65rem; font-weight: 700;
    font-family: 'JetBrains Mono', monospace;
    float: right; margin-top: .05rem;
}
.doc-row-utterance {
    font-family: 'JetBrains Mono', monospace;
    font-size: .82rem; color: #111827;
}
.doc-row-meta { font-size: .75rem; color: #6b7280; margin-top: .2rem; }

/* ── File pill ── */
.file-pill {
    display: inline-flex; align-items: center; gap: .35rem;
    background: #eff6ff; border: 1px solid #bfdbfe;
    border-radius: 999px; padding: .22rem .7rem;
    font-size: .76rem; color: #2563eb;
    font-family: 'JetBrains Mono', monospace;
}

/* ── Key status pill ── */
.key-ok   { display:inline-block; padding:.2rem .65rem; border-radius:999px;
            background:#dcfce7; color:#15803d; font-size:.75rem; font-weight:700;
            border:1px solid #bbf7d0; }
.key-miss { display:inline-block; padding:.2rem .65rem; border-radius:999px;
            background:#fee2e2; color:#b91c1c; font-size:.75rem; font-weight:700;
            border:1px solid #fca5a5; }
</style>
""", unsafe_allow_html=True)


# ── Session state ─────────────────────────────────────────────────────────────
for k, v in [("engine", None), ("history", []), ("eng_cfg", {}), ("file_info", None)]:
    if k not in st.session_state:
        st.session_state[k] = v


# ── API key resolution (env only — no UI input) ───────────────────────────────
def _resolve_key(provider: str) -> str:
    env_var = "GOOGLE_API_KEY" if provider == "gemini" else "OPENAI_API_KEY"
    return os.environ.get(env_var, "").strip()


def _key_status_html(provider: str) -> str:
    key = _resolve_key(provider)
    env_var = "GOOGLE_API_KEY" if provider == "gemini" else "OPENAI_API_KEY"
    if key:
        return f'<span class="key-ok">✓ {env_var} loaded</span>'
    return f'<span class="key-miss">✗ {env_var} not set</span>'


# ── Retrieved-docs helper (clean, no dark theme) ──────────────────────────────
def _doc_rows(hits: list[dict]) -> str:
    html = ""
    for h in hits:
        intent_txt = h["intent"] or "(no intent)"
        area_txt   = h["area"]   or "—"
        html += (
            f'<div class="doc-row">'
            f'<div class="doc-row-rank">{h["rank"]}</div>'
            f'<div class="doc-row-utterance">"{h["utterance"]}"</div>'
            f'<div class="doc-row-meta">🎯 {intent_txt} &nbsp;·&nbsp; 📂 {area_txt}</div>'
            f'</div>'
        )
    return html


# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown('<div class="logo-title">🎯 IntentRAG</div>', unsafe_allow_html=True)
    st.markdown('<div class="logo-sub">BM25 · Vector Search · LLM</div>', unsafe_allow_html=True)
    st.divider()

    # ── 1. Provider ───────────────────────────────────────────────────────
    st.markdown('<div class="section-lbl">🤖 LLM Provider</div>', unsafe_allow_html=True)
    provider = st.selectbox(
        "Provider",
        ["gemini", "openai"],
        format_func=lambda x: "Google Gemini Flash" if x == "gemini" else "OpenAI GPT-4o-mini",
        label_visibility="collapsed",
    )
    # Show key status (read-only — from env)
    st.markdown(_key_status_html(provider), unsafe_allow_html=True)

    st.divider()

    # ── 2. File Upload ────────────────────────────────────────────────────
    st.markdown('<div class="section-lbl">📂 Training Data</div>', unsafe_allow_html=True)
    uploaded = st.file_uploader(
        "Upload file",
        type=["csv", "xlsx", "xls", "pdf"],
        help="CSV / Excel needs Utterance + Intent columns. PDF lines become utterances.",
        label_visibility="collapsed",
    )
    if uploaded:
        icon = {"csv": "📊", "xlsx": "📗", "xls": "📗", "pdf": "📄"}.get(
            uploaded.name.rsplit(".", 1)[-1].lower(), "📄"
        )
        st.markdown(
            f'<div class="file-pill">{icon} {uploaded.name} ({uploaded.size//1024} KB)</div>',
            unsafe_allow_html=True,
        )
        st.session_state.file_info = {"name": uploaded.name, "size": uploaded.size}

    st.divider()

    # ── 3. Search Settings ────────────────────────────────────────────────
    st.markdown('<div class="section-lbl">🔍 Search Settings</div>', unsafe_allow_html=True)

    strategy = st.radio(
        "Search Strategy",
        ["hybrid", "simple"],
        format_func=lambda x: "⚡ Hybrid  (BM25 + Semantic)" if x == "hybrid"
                               else "🔭 Simple  (Semantic only)",
    )
    alpha = 0.6
    if strategy == "hybrid":
        alpha = st.slider("Semantic weight (α)", 0.0, 1.0, 0.6, 0.05,
                          help="1.0 = full semantic · 0.0 = full BM25")
        st.caption(f"Semantic {int(alpha*100)}%  ·  BM25 {int((1-alpha)*100)}%")

    top_k = st.slider("Top-K results", 1, 10, 1,
                      help="How many training utterances to retrieve per query")

    st.divider()

    # ── 4. Load Engine ────────────────────────────────────────────────────
    can_load = uploaded is not None and bool(_resolve_key(provider))
    load_btn = st.button(
        "🚀 Build Index & Load Engine",
        use_container_width=True,
        disabled=not can_load,
    )
    if uploaded is None:
        st.caption("⬆️ Upload a file to continue.")
    elif not _resolve_key(provider):
        env_var = "GOOGLE_API_KEY" if provider == "gemini" else "OPENAI_API_KEY"
        st.caption(f"⚠️ Set `{env_var}` in your `.env` file.")

    if load_btn and can_load:
        cfg = {"file": uploaded.name, "provider": provider,
               "strategy": strategy, "alpha": alpha}
        with st.spinner("Building vector index…"):
            try:
                from core import IntentRAGEngine
                uploaded.seek(0)
                engine = IntentRAGEngine.from_file(
                    source=uploaded,
                    file_name=uploaded.name,
                    llm_provider=provider,
                    api_key=_resolve_key(provider),
                    alpha=alpha,
                    strategy=strategy,
                )
                st.session_state.engine  = engine
                st.session_state.eng_cfg = cfg
                st.success(
                    f"✅ {engine.total_docs} utterances · "
                    f"{len(engine.intent_list)} intents indexed"
                )
            except Exception as e:
                st.error(f"❌ {e}")

    st.divider()
    st.caption("IntentRAG v3.0 · LangChain + ChromaDB")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN PANEL
# ─────────────────────────────────────────────────────────────────────────────
eng = st.session_state.engine

# ── Page header ──────────────────────────────────────────────────────────────
col_h, col_s = st.columns([4, 1])
with col_h:
    st.markdown("## Intent Classification")
    fi = st.session_state.file_info
    st.caption(
        f"📄 {fi['name']}" if fi
        else "Upload a file in the sidebar → click **Build Index & Load Engine**"
    )
with col_s:
    if eng:
        st.success("● Engine Ready")
    else:
        st.warning("● Engine Offline")

st.divider()

if not eng:
    st.info(
        "### 👋 Getting Started\n\n"
        "1. Set `GOOGLE_API_KEY` or `OPENAI_API_KEY` in your `.env` file\n"
        "2. Upload a **CSV / Excel / PDF** training file in the sidebar\n"
        "3. Pick **Search Strategy** and **Top-K**\n"
        "4. Click **Build Index & Load Engine**\n\n"
        "Your file needs `Utterance` and `Intent` columns."
    )
    st.stop()

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_single, tab_batch, tab_explore, tab_history = st.tabs([
    "🔍 Single Query", "📦 Batch Query", "🗂️ Explore Data", "📜 History"
])


# ════════════════════════════════════════════════════════════════════════════
# TAB 1 – Single Query
# ════════════════════════════════════════════════════════════════════════════
with tab_single:
    # Per-query controls
    ctrl_c1, ctrl_c2 = st.columns([2, 1])
    with ctrl_c1:
        q_strategy = st.radio(
            "Strategy",
            ["— Use sidebar default —", "hybrid", "simple"],
            horizontal=True,
            label_visibility="collapsed",
            key="q_strategy",
        )
    with ctrl_c2:
        q_topk = st.number_input(
            "Top-K", min_value=1, max_value=10, value=top_k, key="q_topk",
        )

    active_strategy = eng.strategy if q_strategy == "— Use sidebar default —" else q_strategy

    # Query input
    query = st.text_input(
        "Ask a question",
        placeholder="e.g. How do I reset my password?",
        key="single_query",
    )

    run_btn = st.button("🎯 Classify Intent", type="primary", key="run_single")

    if run_btn and query.strip():
        with st.spinner("Classifying…"):
            try:
                t0     = time.time()
                result = eng.predict(query.strip(), strategy=active_strategy, top_k=int(q_topk))
                ms     = int((time.time() - t0) * 1000)
                conf        = result["confidence"]
                strat_used  = result["strategy"]

                # ── Clean result card ────────────────────────────────────
                st.markdown(
                    f'<div class="result-card">'
                    f'<div class="result-intent">{result["intent"]}'
                    f'<span class="badge badge-{conf}">{conf}</span>'
                    f'<span class="strat-badge strat-{strat_used}">{strat_used}</span>'
                    f'</div>'
                    f'<div class="result-meta">'
                    f'📂 {result["area"] or "—"} &nbsp;·&nbsp; ⏱ {ms} ms'
                    f'</div>'
                    f'<div class="result-reasoning">💬 {result["reasoning"]}</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

                # ── Retrieved Documents (clean rows, no dark theme) ──────
                if result["retrieved"]:
                    st.markdown(f"**Top-{result['top_k']} Retrieved Matches**")
                    st.markdown(
                        _doc_rows(result["retrieved"]),
                        unsafe_allow_html=True,
                    )

                # Save to history
                st.session_state.history.append({
                    "Query":      query.strip(),
                    "Intent":     result["intent"],
                    "Confidence": conf,
                    "Area":       result["area"],
                    "Strategy":   strat_used,
                    "Top-K":      result["top_k"],
                    "Latency ms": ms,
                })

            except Exception as e:
                st.error(f"❌ Prediction failed: {e}")

    elif run_btn:
        st.warning("Please enter a query.")


# ════════════════════════════════════════════════════════════════════════════
# TAB 2 – Batch
# ════════════════════════════════════════════════════════════════════════════
with tab_batch:
    st.markdown("#### Classify multiple queries at once")

    b_c1, b_c2 = st.columns([2, 1])
    with b_c1:
        b_strategy = st.radio(
            "Strategy", ["hybrid", "simple"], horizontal=True, key="b_strat",
            format_func=lambda x: "⚡ Hybrid" if x == "hybrid" else "🔭 Simple",
        )
    with b_c2:
        b_topk = st.number_input("Top-K", min_value=1, max_value=10, value=1, key="b_topk")

    st.caption("One query per line (max 50):")
    batch_text = st.text_area(
        "Queries", height=160,
        placeholder="How do I reset my password?\nWhat is the 401k policy?\nBook a room",
        label_visibility="collapsed",
    )
    run_batch = st.button("🚀 Run Batch", type="primary")

    if run_batch and batch_text.strip():
        queries = [q.strip() for q in batch_text.strip().splitlines() if q.strip()][:50]
        rows, errors = [], []
        prog      = st.progress(0)
        status_ph = st.empty()

        for i, q in enumerate(queries):
            status_ph.caption(f"Processing {i+1}/{len(queries)}: {q[:60]}…")
            try:
                r = eng.predict(q, strategy=b_strategy, top_k=int(b_topk))
                rows.append({
                    "Query":             q,
                    "Intent":            r["intent"],
                    "Confidence":        r["confidence"],
                    "Area":              r["area"],
                    "Strategy":          r["strategy"],
                    "Top-K":             r["top_k"],
                    "Matched Utterance": r["retrieved_utterance"],
                })
            except Exception as e:
                errors.append(f"Row {i+1} — {q[:40]}: {e}")
            prog.progress((i + 1) / len(queries))

        prog.empty(); status_ph.empty()

        if rows:
            df_out = pd.DataFrame(rows)
            st.success(f"✅ Classified {len(rows)} queries")

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total",     len(df_out))
            c2.metric("High 🟢",   int((df_out["Confidence"] == "high").sum()))
            c3.metric("Medium 🟡", int((df_out["Confidence"] == "medium").sum()))
            c4.metric("Low 🔴",    int((df_out["Confidence"] == "low").sum()))

            st.dataframe(df_out, use_container_width=True)
            st.download_button(
                "⬇️ Download CSV",
                df_out.to_csv(index=False).encode(),
                "batch_results.csv", "text/csv",
            )
            for r in rows:
                st.session_state.history.append({
                    "Query": r["Query"], "Intent": r["Intent"],
                    "Confidence": r["Confidence"], "Area": r["Area"],
                    "Strategy": r["Strategy"], "Top-K": r["Top-K"],
                    "Latency ms": "—",
                })

        if errors:
            with st.expander(f"⚠️ {len(errors)} error(s)"):
                for e in errors:
                    st.code(e)

    elif run_batch:
        st.warning("Enter at least one query.")


# ════════════════════════════════════════════════════════════════════════════
# TAB 3 – Explore Data
# ════════════════════════════════════════════════════════════════════════════
with tab_explore:
    st.markdown("#### Training Data Explorer")

    df = eng.df.copy()
    e1, e2 = st.columns(2)
    with e1:
        if "Area" in df.columns:
            sel_area = st.selectbox(
                "Filter by area",
                ["All"] + sorted(df["Area"].dropna().unique().tolist()),
            )
            if sel_area != "All":
                df = df[df["Area"] == sel_area]
    with e2:
        term = st.text_input("Search utterances", placeholder="keyword…")
        if term:
            df = df[df["Utterance"].str.lower().str.contains(term.lower(), na=False)]

    st.caption(f"Showing {len(df)} of {eng.total_docs} records")
    cols_show = [c for c in ["Area", "Utterance", "Intent"] if c in df.columns]
    st.dataframe(df[cols_show].reset_index(drop=True), use_container_width=True)

    ch1, ch2 = st.columns(2)
    with ch1:
        if "Area" in eng.df.columns:
            st.markdown("**By Area**")
            st.bar_chart(eng.df["Area"].value_counts())
    with ch2:
        if "Intent" in eng.df.columns:
            st.markdown("**Top 10 Intents**")
            top_intents = eng.df["Intent"].value_counts().head(10)
            if not top_intents.empty:
                st.bar_chart(top_intents)


# ════════════════════════════════════════════════════════════════════════════
# TAB 4 – History
# ════════════════════════════════════════════════════════════════════════════
with tab_history:
    st.markdown("#### Query History")
    if st.session_state.history:
        df_hist = pd.DataFrame(st.session_state.history)
        st.caption(f"{len(df_hist)} queries this session")

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total",           len(df_hist))
        m2.metric("High conf 🟢",    int((df_hist["Confidence"] == "high").sum()))
        m3.metric("Unique intents",  df_hist["Intent"].nunique())
        m4.metric("Hybrid queries",  int((df_hist.get("Strategy", pd.Series(dtype=str)) == "hybrid").sum()))

        st.dataframe(df_hist, use_container_width=True)

        h1, h2 = st.columns(2)
        with h1:
            if st.button("🗑️ Clear history"):
                st.session_state.history = []
                st.rerun()
        with h2:
            st.download_button(
                "⬇️ Export CSV",
                df_hist.to_csv(index=False).encode(),
                "history.csv", "text/csv",
            )
    else:
        st.info("No queries yet — run a prediction to see history here.")
