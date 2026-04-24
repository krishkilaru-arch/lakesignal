"""LakeSignal — Streamlit Community Cloud edition.

Public-facing dashboard backed by the same lakesignal.core.* Delta tables.
Custom dark UI matching the Databricks FastAPI app.
"""
import streamlit as st
import pandas as pd

st.set_page_config(
    page_title="LakeSignal",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Custom CSS matching the FastAPI app ─────────────────────────────────────
CUSTOM_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

:root {
    --bg: #0b0d10; --panel: #14171c; --panel-2: #1b1f26; --panel-3: #222830;
    --text: #e7ecf1; --muted: #8a93a0; --dim: #555d6b;
    --pos: #22c55e; --pos-bg: rgba(34,197,94,0.08);
    --neg: #ef4444; --neg-bg: rgba(239,68,68,0.08);
    --neu: #94a3b8; --neu-bg: rgba(148,163,184,0.06);
    --accent: #60a5fa; --accent-bg: rgba(96,165,250,0.08);
    --border: #242a33; --border-2: #2e343d;
    --radius: 8px;
}

/* Hide Streamlit defaults */
#MainMenu, footer, header { visibility: hidden; }
.stDeployButton { display: none; }
div[data-testid="stToolbar"] { display: none; }
.block-container { padding-top: 0 !important; max-width: 1600px; }
div[data-testid="stSidebar"] { background: var(--panel) !important; }

/* Override Streamlit backgrounds */
.stApp { background-color: var(--bg) !important; font-family: Inter, -apple-system, sans-serif; }
section[data-testid="stSidebar"] { background: var(--panel) !important; }

/* Nav bar */
.nav-bar {
    display: flex; align-items: center; gap: 0; padding: 0 24px;
    border-bottom: 1px solid var(--border); background: var(--panel);
    margin: 0 -1rem; padding: 0 24px;
}
.nav-bar .logo { font-size: 16px; font-weight: 700; padding: 12px 16px 12px 0;
    border-right: 1px solid var(--border); margin-right: 8px; color: var(--text); }
.nav-bar .logo span { color: var(--accent); }
.nav-bar a { color: var(--muted); text-decoration: none; padding: 12px 16px;
    font-size: 13px; font-weight: 500; border-bottom: 2px solid transparent; }
.nav-bar a:hover { color: var(--text); }
.nav-bar a.active { color: var(--accent); border-bottom-color: var(--accent); }

/* Stat cards row */
.stats-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
    gap: 12px; padding: 16px 0; }
.stat-card { background: var(--panel-2); border: 1px solid var(--border); border-radius: var(--radius);
    padding: 12px 14px; }
.stat-card .label { font-size: 10px; text-transform: uppercase; letter-spacing: 0.7px;
    color: var(--muted); margin-bottom: 2px; }
.stat-card .value { font-size: 22px; font-weight: 700;
    font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
.stat-card.positive .value { color: var(--pos); }
.stat-card.negative .value { color: var(--neg); }
.stat-card.neutral .value { color: var(--neu); }
.stat-card.accent .value { color: var(--accent); }

/* Impact table */
.impact-table { width: 100%; border-collapse: collapse; font-size: 12px;
    border: 1px solid var(--border); border-radius: var(--radius); overflow: hidden; }
.impact-table th { padding: 8px 10px; text-align: left; color: var(--muted); font-weight: 600;
    background: var(--panel); font-size: 11px; letter-spacing: 0.3px; white-space: nowrap;
    border-bottom: 2px solid var(--border); }
.impact-table td { padding: 8px 10px; border-bottom: 1px solid var(--border); vertical-align: top; }
.impact-table tr:hover td { background: var(--panel); }

.dir { font-weight: 700; font-size: 11px; text-transform: uppercase; letter-spacing: 0.3px; }
.dir.positive { color: var(--pos); }
.dir.negative { color: var(--neg); }
.dir.neutral { color: var(--neu); }
.ticker { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-weight: 700; font-size: 12px; }
.num { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; text-align: right; }

.mag-bar { display: inline-block; width: 50px; height: 5px; background: var(--panel-3);
    border-radius: 3px; overflow: hidden; vertical-align: middle; margin-right: 5px; }
.mag-bar > span { display: block; height: 100%; border-radius: 3px; }
.mag-bar.positive > span { background: var(--pos); }
.mag-bar.negative > span { background: var(--neg); }
.mag-bar.neutral > span { background: var(--neu); }

.tag-chip { background: var(--panel-3); color: var(--muted); padding: 2px 6px; border-radius: 4px;
    font-size: 10px; border: 1px solid var(--border); display: inline-block; margin: 1px; }

.headline-text { color: var(--text); font-size: 12px; }
.source-badge { font-size: 10px; color: var(--dim); text-transform: uppercase; letter-spacing: 0.3px; }
.rationale-text { color: var(--muted); font-size: 12px; padding: 4px 0 8px 0; line-height: 1.5; }

/* Verdict badges */
.verdict-correct { background: var(--pos-bg); color: var(--pos); padding: 3px 8px;
    border-radius: 4px; font-size: 11px; font-weight: 600; }
.verdict-wrong { background: var(--neg-bg); color: var(--neg); padding: 3px 8px;
    border-radius: 4px; font-size: 11px; font-weight: 600; }
.verdict-pending { background: var(--neu-bg); color: var(--neu); padding: 3px 8px;
    border-radius: 4px; font-size: 11px; font-weight: 600; }

/* Override Streamlit inputs */
.stTextInput input, .stSelectbox select, div[data-baseweb="select"] {
    background: var(--panel-2) !important; color: var(--text) !important;
    border-color: var(--border) !important; }
.stTextInput input:focus { border-color: var(--accent) !important; }

/* Footer */
.ls-footer { color: var(--dim); font-size: 11px; text-align: center;
    padding: 20px 0; letter-spacing: 0.2px; }
.ls-footer code { color: var(--muted); }

/* URL bar section */
.url-section { background: var(--panel); border: 1px solid var(--border); border-radius: var(--radius);
    padding: 16px; margin: 12px 0; }
.url-section h4 { font-size: 13px; color: var(--text); margin-bottom: 8px; }

/* Scorecard grid */
.scorecard { background: var(--panel-2); border: 1px solid var(--border); border-radius: var(--radius);
    padding: 10px 14px; text-align: center; }
.scorecard .sc-ticker { font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-weight: 700; font-size: 14px; }
.scorecard .sc-detail { font-size: 11px; color: var(--muted); }
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


# ── Navigation ──────────────────────────────────────────────────────────────
if "page" not in st.session_state:
    st.session_state.page = "dashboard"

def nav_bar():
    page = st.session_state.page
    d_cls = "active" if page == "dashboard" else ""
    t_cls = "active" if page == "track_record" else ""
    a_cls = "active" if page == "about" else ""
    st.markdown(f"""
    <div class="nav-bar">
        <div class="logo">⚡ <span>Lake</span>Signal</div>
        <a class="{d_cls}" href="?page=dashboard" target="_self">Dashboard</a>
        <a class="{t_cls}" href="?page=track_record" target="_self">Track Record</a>
        <a class="{a_cls}" href="?page=about" target="_self">About</a>
    </div>
    """, unsafe_allow_html=True)

# Read page from URL params
params = st.query_params
if "page" in params:
    st.session_state.page = params["page"]

nav_bar()


# ════════════════════════════════════════════════════════════════════════════
#  DASHBOARD
# ════════════════════════════════════════════════════════════════════════════
def render_dashboard():
    import db
    import analyzer

    # Summary cards
    stats = db.get_impact_stats()
    total = stats.get("total", 0)
    avg_mag = stats.get("avg_magnitude", 0)
    pos = stats.get("positive", 0)
    neg = stats.get("negative", 0)
    neu = stats.get("neutral", 0)

    st.markdown(f"""
    <div class="stats-row">
        <div class="stat-card accent"><div class="label">Total Impacts</div><div class="value">{total}</div></div>
        <div class="stat-card"><div class="label">Avg Magnitude</div><div class="value">{avg_mag:.1f}</div></div>
        <div class="stat-card positive"><div class="label">Positive</div><div class="value">{pos}</div></div>
        <div class="stat-card negative"><div class="label">Negative</div><div class="value">{neg}</div></div>
        <div class="stat-card neutral"><div class="label">Neutral</div><div class="value">{neu}</div></div>
    </div>
    """, unsafe_allow_html=True)

    # URL Analysis bar
    with st.container():
        col_url, col_btn = st.columns([5, 1])
        url_input = col_url.text_input("🔗 Analyze a news URL", placeholder="https://www.reuters.com/...", label_visibility="collapsed")
        analyze_clicked = col_btn.button("🚀 Analyze", use_container_width=True)

    if analyze_clicked and url_input:
        with st.spinner("Fetching & scoring..."):
            try:
                result = analyzer.analyze_url(url_input.strip(), persist=True)
                if result.get("impacts"):
                    st.success(f"✅ Found {len(result['impacts'])} impact(s) for {', '.join(result['impacts'].keys())}")
                else:
                    st.warning(result.get("note", "No tickers resolved."))
            except Exception as e:
                st.error(f"Error: {e}")

    # Add Ticker expander
    with st.expander("➕ Add Custom Ticker"):
        tc1, tc2, tc3, tc4 = st.columns(4)
        new_sym = tc1.text_input("Symbol", placeholder="SLV").upper().strip()
        new_name = tc2.text_input("Company", placeholder="iShares Silver Trust")
        new_sector = tc3.text_input("Sector", placeholder="Commodities")
        if tc4.button("Add", use_container_width=True):
            if new_sym:
                db.add_ticker(new_sym, new_name, new_sector)
                st.success(f"✅ {new_sym} added")

    # Filters
    fc1, fc2, fc3, fc4 = st.columns(4)
    f_ticker = fc1.text_input("🏷️ Ticker", placeholder="e.g. AAPL").upper().strip() or None
    f_direction = fc2.selectbox("↕️ Direction", ["All", "positive", "negative", "neutral"])
    f_mag = fc3.slider("Min Magnitude", 1, 10, 1)
    f_search = fc4.text_input("🔍 Search headline", placeholder="...").strip() or None

    impacts = db.get_impacts(
        ticker=f_ticker,
        direction=f_direction if f_direction != "All" else None,
        min_magnitude=f_mag if f_mag > 1 else None,
        search=f_search,
        limit=200,
    )

    if not impacts:
        st.markdown('<div style="padding:48px;text-align:center;color:var(--muted)">No impacts match your filters.</div>', unsafe_allow_html=True)
        return

    # Build HTML table
    rows_html = ""
    for imp in impacts:
        d = imp.get("direction", "neutral")
        mag = imp.get("magnitude", 0)
        pct = min(mag * 10, 100)
        ticker = imp.get("ticker_symbol", "")
        sent = imp.get("sentiment_score", 0)
        m1d = imp.get("predicted_move_pct_1d", 0)
        m5d = imp.get("predicted_move_pct_5d", 0)
        conf = imp.get("confidence", 0)
        headline = imp.get("headline", "")[:80]
        source = imp.get("source", "")
        tags = imp.get("risk_tags", [])
        if isinstance(tags, str):
            import json
            try: tags = json.loads(tags)
            except: tags = []
        tags_html = "".join(f'<span class="tag-chip">{t}</span>' for t in (tags or []))
        rationale = imp.get("rationale", "")

        rows_html += f"""
        <tr>
            <td><span class="ticker">{ticker}</span></td>
            <td><span class="dir {d}">{d}</span></td>
            <td class="num"><div class="mag-bar {d}"><span style="width:{pct}%"></span></div>{mag}</td>
            <td class="num">{sent:+.2f}</td>
            <td class="num">{m1d:+.1f}%</td>
            <td class="num">{m5d:+.1f}%</td>
            <td class="num">{conf:.0%}</td>
            <td>{tags_html}</td>
            <td class="headline-text">{headline}</td>
            <td class="source-badge">{source}</td>
        </tr>
        <tr class="rationale-row" style="display:table-row"><td colspan="10"><div class="rationale-text">💡 {rationale}</div></td></tr>
        """

    st.markdown(f"""
    <div style="overflow-x:auto;margin-top:8px;">
    <table class="impact-table">
    <thead><tr>
        <th>Ticker</th><th>Direction</th><th>Magnitude</th><th>Sentiment</th>
        <th>1d %</th><th>5d %</th><th>Conf</th><th>Risk Tags</th><th>Headline</th><th>Source</th>
    </tr></thead>
    <tbody>{rows_html}</tbody>
    </table>
    </div>
    """, unsafe_allow_html=True)

    # CSV download
    df = pd.DataFrame(impacts)
    csv = df.to_csv(index=False)
    st.download_button("⬇️ Download CSV", csv, "lakesignal_impacts.csv", "text/csv")


# ════════════════════════════════════════════════════════════════════════════
#  TRACK RECORD
# ════════════════════════════════════════════════════════════════════════════
def render_track_record():
    import db

    dates = db.get_backtest_dates()
    if not dates:
        st.markdown('<div style="padding:48px;text-align:center;color:var(--muted)">No backtest data yet. The daily job runs Mon–Fri at 5 PM ET.</div>', unsafe_allow_html=True)
        return

    selected_date = st.selectbox("📅 Event Date", dates, index=0)

    # Summary stats
    summary = db.get_backtest_summary()
    total = summary.get("total", 0)
    correct = summary.get("correct", 0)
    wrong = summary.get("wrong", 0)
    pending = summary.get("pending", 0)
    acc = (correct / (correct + wrong) * 100) if (correct + wrong) > 0 else 0
    avg_err = summary.get("avg_mag_error")
    avg_err_str = f"{avg_err:.2f}" if avg_err else "—"

    st.markdown(f"""
    <div class="stats-row">
        <div class="stat-card accent"><div class="label">Direction Accuracy</div><div class="value">{acc:.0f}%</div></div>
        <div class="stat-card"><div class="label">Total Predictions</div><div class="value">{total}</div></div>
        <div class="stat-card positive"><div class="label">Correct</div><div class="value">{correct}</div></div>
        <div class="stat-card negative"><div class="label">Wrong</div><div class="value">{wrong}</div></div>
        <div class="stat-card neutral"><div class="label">Pending</div><div class="value">{pending}</div></div>
        <div class="stat-card"><div class="label">Avg Mag Error</div><div class="value">{avg_err_str}</div></div>
    </div>
    """, unsafe_allow_html=True)

    # Results for selected date
    rows = db.get_backtest_results(event_date=selected_date)
    if not rows:
        st.markdown(f'<div style="padding:24px;text-align:center;color:var(--muted)">No results for {selected_date}.</div>', unsafe_allow_html=True)
        return

    # Per-ticker scorecards
    tickers = {}
    for r in rows:
        t = r.get("ticker", "")
        if t not in tickers:
            tickers[t] = {"count": 0, "dir": r.get("direction_predicted", "")}
        tickers[t]["count"] += 1

    cards_html = ""
    for t, info in tickers.items():
        d = info["dir"]
        icon = {"positive": "🟢", "negative": "🔴", "neutral": "⚪"}.get(d, "⚪")
        cards_html += f'<div class="scorecard"><div class="sc-ticker">{icon} {t}</div><div class="sc-detail">{info["count"]} prediction(s)</div></div>'

    st.markdown(f'<div class="stats-row">{cards_html}</div>', unsafe_allow_html=True)

    # Detailed table
    table_rows = ""
    for r in rows:
        dc1d = r.get("direction_correct_1d")
        if dc1d is True:
            verdict = '<span class="verdict-correct">✅ Correct</span>'
        elif dc1d is False:
            verdict = '<span class="verdict-wrong">❌ Wrong</span>'
        else:
            verdict = '<span class="verdict-pending">⏳ Pending</span>'

        d = r.get("direction_predicted", "")
        ticker = r.get("ticker", "")
        mag = r.get("magnitude_predicted", 0)
        pred1d = r.get("predicted_move_1d", 0) or 0
        act1d = r.get("actual_move_1d_pct")
        act1d_str = f"{act1d:.2f}%" if act1d is not None else "—"
        mag_err = r.get("magnitude_error_1d")
        mag_err_str = f"{mag_err:.2f}" if mag_err is not None else "—"
        conf = r.get("confidence", 0) or 0
        headline = (r.get("headline") or "")[:60]

        table_rows += f"""
        <tr>
            <td>{verdict}</td>
            <td><span class="ticker">{ticker}</span></td>
            <td><span class="dir {d}">{d}</span></td>
            <td class="num">{mag}</td>
            <td class="num">{pred1d:+.1f}%</td>
            <td class="num">{act1d_str}</td>
            <td class="num">{mag_err_str}</td>
            <td class="num">{conf:.0%}</td>
            <td class="headline-text">{headline}</td>
        </tr>"""

    st.markdown(f"""
    <div style="overflow-x:auto;margin-top:12px;">
    <table class="impact-table">
    <thead><tr>
        <th>Verdict</th><th>Ticker</th><th>Predicted</th><th>Mag</th>
        <th>Pred 1d</th><th>Actual 1d</th><th>Mag Err</th><th>Conf</th><th>Headline</th>
    </tr></thead>
    <tbody>{table_rows}</tbody>
    </table>
    </div>
    """, unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════════════════
#  ABOUT
# ════════════════════════════════════════════════════════════════════════════
def render_about():
    st.markdown("""
    <div style="max-width:800px;margin:24px auto;color:var(--text);">
    <h1 style="font-size:28px;font-weight:700;">📰 About LakeSignal</h1>

    <h3 style="margin-top:24px;color:var(--accent);">What is LakeSignal?</h3>
    <p style="color:var(--muted);line-height:1.8;">
    LakeSignal is a <strong style="color:var(--text)">Databricks-native news-to-ticker impact scoring system</strong>.
    It ingests financial news from RSS feeds, resolves mentioned stock tickers,
    and uses AI (Databricks Foundation Model APIs) to predict short-term stock price impact for each ticker.
    </p>

    <h3 style="margin-top:24px;color:var(--accent);">How it works</h3>
    <div style="color:var(--muted);line-height:1.8;">
    <p><strong style="color:var(--text)">1. Ingest</strong> — RSS feeds from Yahoo Finance, Reuters, MarketWatch, CNBC, SEC EDGAR, and Seeking Alpha are polled every 5 minutes.</p>
    <p><strong style="color:var(--text)">2. Resolve</strong> — Each article is matched to S&P 500 tickers using symbol patterns, company name matching, and alias lookup.</p>
    <p><strong style="color:var(--text)">3. Score</strong> — For each (article, ticker) pair, the AI produces direction, magnitude (1–10), predicted 1d/5d moves, confidence, risk tags, and a rationale.</p>
    <p><strong style="color:var(--text)">4. Backtest</strong> — Daily after market close, actual stock prices are fetched and compared against predictions.</p>
    </div>

    <h3 style="margin-top:24px;color:var(--accent);">Architecture</h3>
    <pre style="background:var(--panel-2);border:1px solid var(--border);border-radius:8px;padding:16px;color:var(--muted);font-size:12px;overflow-x:auto;">
RSS Feeds ──▶ Ingest Notebook ──▶ news_events ──▶ impact_analysis
                                      │                  │
User URL ──▶ /analyze/url ────────────┘                  │
                                                         ▼
                                 Backtest Job + yfinance ──▶ backtest_results
    </pre>

    <h3 style="margin-top:24px;color:var(--accent);">Field Glossary</h3>
    <table class="impact-table" style="margin-top:8px;">
    <tr><th style="width:160px">Field</th><th>Meaning</th></tr>
    <tr><td><strong>Direction</strong></td><td style="color:var(--muted)">Whether the AI expects the stock to go up, down, or stay flat</td></tr>
    <tr><td><strong>Magnitude</strong></td><td style="color:var(--muted)">Impact strength on a 1–10 scale (most news is 2–4)</td></tr>
    <tr><td><strong>Predicted Move 1d</strong></td><td style="color:var(--muted)">Expected percentage change over 1 trading day</td></tr>
    <tr><td><strong>Predicted Move 5d</strong></td><td style="color:var(--muted)">Expected percentage change over 5 trading days</td></tr>
    <tr><td><strong>Confidence</strong></td><td style="color:var(--muted)">How confident the AI is in its prediction (0–100%)</td></tr>
    <tr><td><strong>Risk Tags</strong></td><td style="color:var(--muted)">Categories: earnings, m&amp;a, regulatory, macro, etc.</td></tr>
    <tr><td><strong>Rationale</strong></td><td style="color:var(--muted)">One-sentence plain-English explanation</td></tr>
    </table>

    <h3 style="margin-top:24px;color:var(--accent);">Who benefits?</h3>
    <div style="color:var(--muted);line-height:1.8;">
    <p>🔹 <strong style="color:var(--text)">Traders</strong> — fast signal on which stocks are news-impacted right now</p>
    <p>🔹 <strong style="color:var(--text)">Risk managers</strong> — early warning on portfolio exposure to breaking news</p>
    <p>🔹 <strong style="color:var(--text)">Analysts</strong> — structured, comparable AI scoring across hundreds of tickers</p>
    <p>🔹 <strong style="color:var(--text)">Data teams</strong> — clean, governed data in Unity Catalog for downstream models</p>
    </div>

    <div class="ls-footer" style="margin-top:32px;">
        Built on <code>Databricks</code> · <code>Delta Lake</code> · <code>Foundation Model APIs</code> · <code>Streamlit</code>
    </div>
    </div>
    """, unsafe_allow_html=True)


# ── Router ──────────────────────────────────────────────────────────────────
page = st.session_state.page
if page == "dashboard":
    render_dashboard()
elif page == "track_record":
    render_track_record()
else:
    render_about()

# Footer
st.markdown('<div class="ls-footer">LakeSignal v0.1 · Powered by Databricks</div>', unsafe_allow_html=True)
