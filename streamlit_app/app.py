"""LakeSignal — Streamlit Community Cloud edition.

Public-facing dashboard backed by the same lakesignal.core.* Delta tables.
Connects to Databricks Unity Catalog via a service principal.
"""
import streamlit as st
import pandas as pd

st.set_page_config(
    page_title="LakeSignal",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Navigation ────────────────────────────────────────────────────────────
PAGES = {
    "⚡ Dashboard": "dashboard",
    "🎯 Track Record": "track_record",
    "📰 About": "about",
}
page = st.sidebar.radio("Navigate", list(PAGES.keys()), label_visibility="collapsed")


# ══════════════════════════════════════════════════════════════════════════
#  DASHBOARD
# ══════════════════════════════════════════════════════════════════════════
def render_dashboard():
    import db
    import analyzer

    st.title("⚡ LakeSignal Dashboard")
    st.caption("AI-powered news → stock impact scoring · Databricks + Unity Catalog")

    # ── Summary cards ─────────────────────────────────────────────────
    stats = db.get_impact_stats()
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Impacts", stats.get("total", 0))
    c2.metric("Avg Magnitude", f"{stats.get('avg_magnitude', 0):.1f}")
    c3.metric("🟢 Positive", stats.get("positive", 0))
    c4.metric("🔴 Negative", stats.get("negative", 0))
    c5.metric("⚪ Neutral", stats.get("neutral", 0))

    st.divider()

    # ── Analyze URL ───────────────────────────────────────────────────
    with st.expander("🔗 Analyze a News URL", expanded=False):
        url_input = st.text_input("Paste a news article URL", placeholder="https://www.reuters.com/...")
        col_a, col_b = st.columns([1, 4])
        persist = col_a.checkbox("Save to DB", value=True)
        if col_b.button("🚀 Analyze", disabled=not url_input):
            with st.spinner("Fetching & scoring..."):
                try:
                    result = analyzer.analyze_url(url_input.strip(), persist=persist)
                    if result.get("impacts"):
                        st.success(f"Found {len(result['impacts'])} impact(s): {', '.join(result['impacts'].keys())}")
                        for sym, s in result["impacts"].items():
                            dir_icon = {
                                "positive": "🟢", "negative": "🔴", "neutral": "⚪"
                            }.get(s["direction"], "⚪")
                            st.markdown(
                                f"**{dir_icon} {sym}** — {s['direction']} "
                                f"(mag {s['magnitude']}, conf {s['confidence']:.0%}) — "
                                f"_{s['rationale']}_"
                            )
                    else:
                        st.warning(result.get("note", "No tickers resolved from article."))
                except Exception as e:
                    st.error(f"Error: {e}")

    # ── Add Ticker ────────────────────────────────────────────────────
    with st.expander("➕ Add Custom Ticker", expanded=False):
        tc1, tc2, tc3 = st.columns(3)
        new_sym = tc1.text_input("Symbol", placeholder="SLV").upper().strip()
        new_name = tc2.text_input("Company Name", placeholder="iShares Silver Trust")
        new_sector = tc3.text_input("Sector", placeholder="Commodities")
        if st.button("Add Ticker", disabled=not new_sym):
            db.add_ticker(new_sym, new_name, new_sector)
            st.success(f"Ticker {new_sym} added.")

    st.divider()

    # ── Filters ───────────────────────────────────────────────────────
    st.subheader("Impacts")
    fc1, fc2, fc3, fc4 = st.columns(4)
    f_ticker = fc1.text_input("🏷️ Ticker", placeholder="e.g. AAPL").upper().strip() or None
    f_direction = fc2.selectbox("↕️ Direction", ["All", "positive", "negative", "neutral"])
    f_mag = fc3.slider("Min Magnitude", 1, 10, 1)
    f_search = fc4.text_input("🔍 Headline", placeholder="search...").strip() or None

    impacts = db.get_impacts(
        ticker=f_ticker or None,
        direction=f_direction if f_direction != "All" else None,
        min_magnitude=f_mag if f_mag > 1 else None,
        search=f_search,
        limit=200,
    )

    if not impacts:
        st.info("No impacts match your filters.")
        return

    df = pd.DataFrame(impacts)

    # Direction color indicator
    def dir_badge(d):
        return {"positive": "🟢", "negative": "🔴", "neutral": "⚪"}.get(d, d)

    df.insert(0, "", df["direction"].apply(dir_badge))

    # Display columns
    show_cols = [
        "", "ticker_symbol", "direction", "magnitude", "sentiment_score",
        "predicted_move_pct_1d", "predicted_move_pct_5d", "confidence",
        "headline", "source", "analyzed_at",
    ]
    show_cols = [c for c in show_cols if c in df.columns]

    st.dataframe(
        df[show_cols],
        use_container_width=True,
        height=500,
        column_config={
            "": st.column_config.TextColumn("", width=30),
            "ticker_symbol": st.column_config.TextColumn("Ticker", width=70),
            "magnitude": st.column_config.ProgressColumn("Mag", min_value=0, max_value=10, width=80),
            "confidence": st.column_config.ProgressColumn("Conf", min_value=0, max_value=1, format="%.0f%%", width=70),
            "predicted_move_pct_1d": st.column_config.NumberColumn("1d %", format="%.1f%%", width=60),
            "predicted_move_pct_5d": st.column_config.NumberColumn("5d %", format="%.1f%%", width=60),
            "sentiment_score": st.column_config.NumberColumn("Sent", format="%.2f", width=60),
            "headline": st.column_config.TextColumn("Headline", width=300),
        },
        hide_index=True,
    )

    # Expandable rationale
    with st.expander("View rationale for each impact"):
        for _, row in df.iterrows():
            rat = row.get("rationale", "")
            if rat:
                st.markdown(f"**{row.get('ticker_symbol', '')}**: {rat}")

    # CSV download
    csv = df[show_cols].to_csv(index=False)
    st.download_button("⬇️ Download CSV", csv, "lakesignal_impacts.csv", "text/csv")


# ══════════════════════════════════════════════════════════════════════════
#  TRACK RECORD
# ══════════════════════════════════════════════════════════════════════════
def render_track_record():
    import db

    st.title("🎯 Track Record")
    st.caption("How accurate are LakeSignal's predictions vs actual stock moves?")

    # ── Date picker ───────────────────────────────────────────────────
    dates = db.get_backtest_dates()
    if not dates:
        st.info("No backtest data yet. The daily backtest job runs Mon–Fri at 5 PM ET.")
        return

    selected_date = st.selectbox("Event Date", dates, index=0)

    # ── Summary cards ─────────────────────────────────────────────────
    summary = db.get_backtest_summary()
    total = summary.get("total", 0)
    correct = summary.get("correct", 0)
    wrong = summary.get("wrong", 0)
    pending = summary.get("pending", 0)
    acc = (correct / (correct + wrong) * 100) if (correct + wrong) > 0 else 0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Direction Accuracy", f"{acc:.0f}%")
    c2.metric("Total Predictions", total)
    c3.metric("✅ Correct", correct)
    c4.metric("❌ Wrong", wrong)
    c5.metric("⏳ Pending", pending)

    st.divider()

    # ── Results table ─────────────────────────────────────────────────
    rows = db.get_backtest_results(event_date=selected_date)
    if not rows:
        st.info(f"No backtest results for {selected_date}.")
        return

    df = pd.DataFrame(rows)

    # Verdict badge
    def verdict(row):
        if row.get("direction_correct_1d") is True:
            return "✅ Correct"
        elif row.get("direction_correct_1d") is False:
            return "❌ Wrong"
        return "⏳ Pending"

    df["verdict"] = df.apply(verdict, axis=1)

    # Per-ticker scorecard
    st.subheader("Per-Ticker Scorecard")
    ticker_groups = df.groupby("ticker").agg(
        predictions=pd.NamedAgg(column="ticker", aggfunc="count"),
        correct=pd.NamedAgg(column="direction_correct_1d", aggfunc=lambda x: x.sum() if x.dtype == bool else 0),
    ).reset_index()

    cols = st.columns(min(len(ticker_groups), 6))
    for i, (_, trow) in enumerate(ticker_groups.iterrows()):
        col = cols[i % len(cols)]
        pred_dir = df[df["ticker"] == trow["ticker"]]["direction_predicted"].iloc[0]
        icon = {
            "positive": "🟢", "negative": "🔴", "neutral": "⚪"
        }.get(pred_dir, "⚪")
        col.metric(
            f"{icon} {trow['ticker']}",
            f"{trow['predictions']} pred",
        )

    st.divider()

    # Detailed table
    show_cols = [
        "verdict", "ticker", "direction_predicted", "magnitude_predicted",
        "predicted_move_1d", "actual_move_1d_pct", "magnitude_error_1d",
        "confidence", "headline",
    ]
    show_cols = [c for c in show_cols if c in df.columns]

    st.dataframe(
        df[show_cols],
        use_container_width=True,
        height=450,
        column_config={
            "verdict": st.column_config.TextColumn("Verdict", width=90),
            "ticker": st.column_config.TextColumn("Ticker", width=70),
            "direction_predicted": st.column_config.TextColumn("Predicted", width=80),
            "magnitude_predicted": st.column_config.ProgressColumn("Mag", min_value=0, max_value=10, width=70),
            "predicted_move_1d": st.column_config.NumberColumn("Pred 1d%", format="%.1f%%", width=70),
            "actual_move_1d_pct": st.column_config.NumberColumn("Actual 1d%", format="%.2f%%", width=80),
            "magnitude_error_1d": st.column_config.NumberColumn("Mag Err", format="%.2f", width=70),
            "confidence": st.column_config.ProgressColumn("Conf", min_value=0, max_value=1, format="%.0f%%", width=70),
            "headline": st.column_config.TextColumn("Headline", width=300),
        },
        hide_index=True,
    )


# ══════════════════════════════════════════════════════════════════════════
#  ABOUT
# ══════════════════════════════════════════════════════════════════════════
def render_about():
    st.title("📰 About LakeSignal")

    st.markdown("""
    ### What is LakeSignal?

    LakeSignal is a **Databricks-native news-to-ticker impact scoring system**.
    It ingests financial news from RSS feeds, resolves mentioned stock tickers,
    and uses AI (Databricks Foundation Model APIs) to predict short-term stock
    price impact for each ticker.

    ### How it works

    1. **Ingest** — RSS feeds from Yahoo Finance, Reuters, MarketWatch, CNBC,
       SEC EDGAR, and Seeking Alpha are polled every 5 minutes.
    2. **Resolve** — Each article is matched to S&P 500 tickers using symbol
       patterns (`$AAPL`, `(AAPL)`, `NASDAQ:AAPL`), company name matching,
       and alias lookup.
    3. **Score** — For each (article, ticker) pair, the AI produces:
       - **Direction**: positive / negative / neutral
       - **Magnitude**: 1–10 scale of expected impact
       - **Predicted move**: 1-day and 5-day percentage change estimates
       - **Confidence**: 0–1 score
       - **Risk tags**: earnings, m&a, regulatory, macro, etc.
       - **Rationale**: one-sentence explanation
    4. **Backtest** — Daily (Mon–Fri after market close), actual stock prices
       are fetched and compared against predictions to track accuracy.

    ### Architecture

    ```
    RSS Feeds ─▶ Ingest Notebook ─▶ news_events ─▶ impact_analysis
                                        │                  │
    User URL ─▶ /analyze/url ─────────┘                  │
                                                           ▼
                                   Backtest Job + yfinance ─▶ backtest_results
    ```

    All data lives in **Unity Catalog** (`lakesignal.core.*`) as Delta tables.

    ### Field Glossary

    | Field | Meaning |
    | --- | --- |
    | **Direction** | Whether the AI expects the stock to go up, down, or stay flat |
    | **Magnitude** | Impact strength on a 1–10 scale (most news is 2–4) |
    | **Predicted Move 1d** | Expected percentage change over 1 trading day |
    | **Predicted Move 5d** | Expected percentage change over 5 trading days |
    | **Confidence** | How confident the AI is in its prediction (0–100%) |
    | **Risk Tags** | Categories of risk: earnings, m&a, regulatory, macro, etc. |
    | **Rationale** | One-sentence plain-English explanation of the score |

    ### Who benefits?

    - **Traders** — fast signal on which stocks are news-impacted right now
    - **Risk managers** — early warning on portfolio exposure to breaking news
    - **Analysts** — structured, comparable AI scoring across hundreds of tickers
    - **Data teams** — clean, governed data in Unity Catalog for downstream models

    ---
    *Built on Databricks · Delta Lake · Foundation Model APIs · Streamlit*
    """)


# ── Router ────────────────────────────────────────────────────────────────
if PAGES[page] == "dashboard":
    render_dashboard()
elif PAGES[page] == "track_record":
    render_track_record()
else:
    render_about()

# ── Sidebar footer ────────────────────────────────────────────────────────
st.sidebar.divider()
st.sidebar.caption("LakeSignal v0.1 · [GitHub](https://github.com/your-repo) · Powered by Databricks")
