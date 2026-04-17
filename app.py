import streamlit as st
import json
import os
import pandas as pd

REPORTS_DIR = os.path.join(os.path.dirname(__file__), 'reports')
REPORT_PATH = os.path.join(REPORTS_DIR, 'fraud_report.json')

st.set_page_config(
    page_title="Stock Fraud Screener",
    page_icon="🔍",
    layout="wide"
)

# ── Styling ──────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .risk-high   { color: #e74c3c; font-weight: bold; }
    .risk-medium { color: #f39c12; font-weight: bold; }
    .risk-low    { color: #27ae60; font-weight: bold; }
    .metric-box  { background: #1e1e2e; padding: 1rem; border-radius: 8px; text-align: center; }
</style>
""", unsafe_allow_html=True)


# ── Load data ─────────────────────────────────────────────────────────────────
@st.cache_data
def load_report():
    if not os.path.exists(REPORT_PATH):
        return []
    with open(REPORT_PATH) as f:
        return json.load(f)


def refresh():
    st.cache_data.clear()
    st.rerun()


data = load_report()

# ── Header ────────────────────────────────────────────────────────────────────
col_title, col_refresh = st.columns([6, 1])
with col_title:
    st.title("🔍 Stock Fraud Screener")
    st.caption("Powered by SEC EDGAR · All US public companies")
with col_refresh:
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("🔄 Refresh"):
        refresh()

if not data:
    st.warning("No report found. Run `python3 run.py --full` first to generate data.")
    st.stop()

df = pd.DataFrame(data)

# Ensure market_cap column exists (older reports may not have it)
if 'market_cap' not in df.columns:
    df['market_cap'] = None

df['market_cap'] = pd.to_numeric(df['market_cap'], errors='coerce')

# ── Summary metrics (calculated after filters — see below) ───────────────────
METRICS_PLACEHOLDER = st.empty()

# ── Sidebar filters ───────────────────────────────────────────────────────────
st.sidebar.header("Filters")

# Market cap filter
st.sidebar.markdown("**Market Cap**")
preset = st.sidebar.radio(
    "Quick select:",
    ["All sizes", "Micro (<$300M)", "Small ($150M-$1B)", "Mid ($1B-$10B)", "Large (>$10B)", "Custom"],
    index=0,
    horizontal=False
)

preset_ranges = {
    "All sizes":        (0, 10_000_000),
    "Micro (<$300M)":   (0, 300),
    "Small ($150M-$1B)":(150, 1_000),
    "Mid ($1B-$10B)":   (1_000, 10_000),
    "Large (>$10B)":    (10_000, 10_000_000),
    "Custom":           None,
}

if preset != "Custom":
    mc_min_input, mc_max_input = preset_ranges[preset]
else:
    mc_col1, mc_col2 = st.sidebar.columns(2)
    mc_min_input = mc_col1.number_input("Min ($M)", min_value=0, max_value=10_000_000, value=0, step=50)
    mc_max_input = mc_col2.number_input("Max ($M)", min_value=0, max_value=10_000_000, value=10_000_000, step=50)

mc_min = mc_min_input * 1_000_000
mc_max = mc_max_input * 1_000_000

st.sidebar.markdown("---")

risk_filter = st.sidebar.multiselect(
    "Risk Level",
    options=["HIGH RISK", "MEDIUM RISK", "LOW RISK"],
    default=["HIGH RISK", "MEDIUM RISK"]
)

min_score = st.sidebar.slider("Minimum Fraud Score", 0, 100, 0)
min_flags = st.sidebar.selectbox("Minimum Red Flags", [0, 1, 2, 3, 4], index=0)
search = st.sidebar.text_input("Search by ticker or name", "")

st.sidebar.markdown("---")
st.sidebar.markdown("**Score guide:**")
st.sidebar.markdown("- 🔴 70–100: High Risk")
st.sidebar.markdown("- 🟡 45–69: Medium Risk")
st.sidebar.markdown("- 🟢 0–44: Low Risk")

# ── Apply filters ─────────────────────────────────────────────────────────────
filtered = df.copy()

# Market cap filter — only apply to companies that have market cap data
has_mcap = filtered['market_cap'].notna()
in_range = (filtered['market_cap'] >= mc_min) & (filtered['market_cap'] <= mc_max)
filtered = filtered[~has_mcap | in_range]

if risk_filter:
    filtered = filtered[filtered['risk'].isin(risk_filter)]

filtered = filtered[filtered['fraud_score'].fillna(0) >= min_score]
filtered = filtered[filtered['red_flags_count'] >= min_flags]

if search:
    mask = (
        filtered['ticker'].str.contains(search.upper(), na=False) |
        filtered['name'].str.contains(search, case=False, na=False)
    )
    filtered = filtered[mask]

# ── Summary metrics (reflect current filters) ────────────────────────────────
with METRICS_PLACEHOLDER.container():
    st.markdown("---")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Companies", len(filtered))
    c2.metric("🔴 High Risk",   len(filtered[filtered['risk'] == 'HIGH RISK']))
    c3.metric("🟡 Medium Risk", len(filtered[filtered['risk'] == 'MEDIUM RISK']))
    c4.metric("🟢 Low Risk",    len(filtered[filtered['risk'] == 'LOW RISK']))
    st.markdown("---")

st.markdown(f"**Showing {len(filtered)} companies**")

# ── Main table ────────────────────────────────────────────────────────────────
def fmt_market_cap(val):
    if pd.isna(val):
        return 'N/A'
    if val >= 1_000_000_000:
        return f"${val/1_000_000_000:.1f}B"
    return f"${val/1_000_000:.0f}M"

display_cols = {
    'ticker':          'Ticker',
    'risk':            'Risk',
    'fraud_score':     'Score',
    'red_flags_count': 'Flags',
    'market_cap':      'Mkt Cap',
    'beneish_score':   'Beneish',
    'piotroski_score': 'Piotroski',
    'accruals_ratio':  'Accruals',
    'cfd_ratio':       'CF Div.',
    'name':            'Company',
}

table_df = filtered[list(display_cols.keys())].rename(columns=display_cols).copy()
table_df['Score']     = table_df['Score'].round(1)
table_df['Beneish']   = table_df['Beneish'].round(2)
table_df['Accruals']  = table_df['Accruals'].round(3)
table_df['CF Div.']   = table_df['CF Div.'].round(3)
table_df['Mkt Cap']   = table_df['Mkt Cap'].apply(fmt_market_cap)


def color_risk(val):
    if val == 'HIGH RISK':
        return 'color: #e74c3c; font-weight: bold'
    if val == 'MEDIUM RISK':
        return 'color: #f39c12; font-weight: bold'
    return 'color: #27ae60'


def color_score(val):
    if pd.isna(val):
        return ''
    if val >= 70:
        return 'background-color: #5c1010'
    if val >= 45:
        return 'background-color: #5c3a10'
    return ''


styled = (
    table_df.style
    .map(color_risk, subset=['Risk'])
    .map(color_score, subset=['Score'])
    .format({
        'Score':    '{:.1f}',
        'Beneish':  '{:.2f}',
        'Accruals': '{:.3f}',
        'CF Div.':  '{:.3f}',
    }, na_rep='N/A')
)

st.dataframe(styled, use_container_width=True, height=500)

# ── Company detail view ───────────────────────────────────────────────────────
st.markdown("---")
st.subheader("Company Detail")

tickers = filtered['ticker'].dropna().tolist()
selected = st.selectbox("Select a company to inspect", options=tickers)

if selected:
    row = filtered[filtered['ticker'] == selected].iloc[0]

    d1, d2, d3, d4 = st.columns(4)
    d1.markdown(f"**{row['name']}** (`{row['ticker']}`)")
    d2.markdown(f"Risk: **{row['risk']}**")
    d3.markdown(f"Fraud Score: **{row['fraud_score']:.1f} / 100**")
    d4.markdown(f"Mkt Cap: **{fmt_market_cap(row['market_cap'])}**")

    st.markdown("#### Signal Breakdown")
    s1, s2, s3, s4 = st.columns(4)

    beneish_flag   = "🔴" if row.get('beneish_flag')   else "🟢"
    piotroski_flag = "🔴" if row.get('piotroski_weak') else "🟢"
    accruals_flag  = "🔴" if row.get('accruals_flag')  else "🟢"
    cfd_flag       = "🔴" if row.get('cfd_flag')       else "🟢"

    s1.metric(f"{beneish_flag} Beneish M-Score",
              f"{row['beneish_score']:.2f}" if pd.notna(row['beneish_score']) else "N/A",
              help="Above -1.78 = likely earnings manipulation")

    s2.metric(f"{piotroski_flag} Piotroski F-Score",
              f"{int(row['piotroski_score'])}" if pd.notna(row['piotroski_score']) else "N/A",
              help="0-2 = financially weak, 7-9 = strong")

    s3.metric(f"{accruals_flag} Accruals Ratio",
              f"{row['accruals_ratio']:.3f}" if pd.notna(row['accruals_ratio']) else "N/A",
              help="Above 0.05 = earnings not backed by cash")

    s4.metric(f"{cfd_flag} Cash Flow Divergence",
              f"{row['cfd_ratio']:.3f}" if pd.notna(row['cfd_ratio']) else "N/A",
              help="Above 0.25 = large gap between income and cash")

    st.markdown("#### What the signals mean")
    explanations = []
    if row.get('beneish_flag'):
        explanations.append("⚠️ **Beneish flagged**: Earnings may be manipulated — revenue inflation or expense deferral suspected.")
    if row.get('piotroski_weak'):
        explanations.append("⚠️ **Piotroski weak**: Poor financial health — low profitability, high leverage, or deteriorating operations.")
    if row.get('accruals_flag'):
        explanations.append("⚠️ **High accruals**: Reported profit is not being converted to cash — classic early fraud warning sign.")
    if row.get('cfd_flag'):
        explanations.append("⚠️ **Cash flow divergence**: Net income is significantly higher than operating cash flow — unsustainable.")
    if not explanations:
        explanations.append("✅ No major red flags detected for this company.")

    for e in explanations:
        st.markdown(e)
