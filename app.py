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
    st.caption("Targeting $150M–$1B market cap companies · Powered by SEC EDGAR")
with col_refresh:
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("🔄 Refresh"):
        refresh()

if not data:
    st.warning("No report found. Run `python3 run.py --full` first to generate data.")
    st.stop()

df = pd.DataFrame(data)

# ── Summary metrics ───────────────────────────────────────────────────────────
st.markdown("---")
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Companies", len(df))
c2.metric("🔴 High Risk",   len(df[df['risk'] == 'HIGH RISK']))
c3.metric("🟡 Medium Risk", len(df[df['risk'] == 'MEDIUM RISK']))
c4.metric("🟢 Low Risk",    len(df[df['risk'] == 'LOW RISK']))
st.markdown("---")

# ── Sidebar filters ───────────────────────────────────────────────────────────
st.sidebar.header("Filters")

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

st.markdown(f"**Showing {len(filtered)} companies**")

# ── Main table ────────────────────────────────────────────────────────────────
display_cols = {
    'ticker':           'Ticker',
    'risk':             'Risk',
    'fraud_score':      'Score',
    'red_flags_count':  'Flags',
    'beneish_score':    'Beneish',
    'piotroski_score':  'Piotroski',
    'accruals_ratio':   'Accruals',
    'cfd_ratio':        'CF Divergence',
    'name':             'Company',
}

table_df = filtered[list(display_cols.keys())].rename(columns=display_cols)
table_df['Score'] = table_df['Score'].round(1)
table_df['Beneish'] = table_df['Beneish'].round(2)
table_df['Accruals'] = table_df['Accruals'].round(3)
table_df['CF Divergence'] = table_df['CF Divergence'].round(3)


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
    .applymap(color_risk, subset=['Risk'])
    .applymap(color_score, subset=['Score'])
    .format({'Score': '{:.1f}', 'Beneish': '{:.2f}', 'Accruals': '{:.3f}', 'CF Divergence': '{:.3f}'}, na_rep='N/A')
)

st.dataframe(styled, use_container_width=True, height=500)

# ── Company detail view ───────────────────────────────────────────────────────
st.markdown("---")
st.subheader("Company Detail")

tickers = filtered['ticker'].dropna().tolist()
selected = st.selectbox("Select a company to inspect", options=tickers)

if selected:
    row = filtered[filtered['ticker'] == selected].iloc[0]

    d1, d2, d3 = st.columns(3)
    d1.markdown(f"**{row['name']}** (`{row['ticker']}`)")
    d2.markdown(f"Risk: **{row['risk']}**")
    d3.markdown(f"Fraud Score: **{row['fraud_score']:.1f} / 100**")

    st.markdown("#### Signal Breakdown")
    s1, s2, s3, s4 = st.columns(4)

    beneish_flag = "🔴" if row.get('beneish_flag') else "🟢"
    piotroski_flag = "🔴" if row.get('piotroski_weak') else "🟢"
    accruals_flag = "🔴" if row.get('accruals_flag') else "🟢"
    cfd_flag = "🔴" if row.get('cfd_flag') else "🟢"

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
