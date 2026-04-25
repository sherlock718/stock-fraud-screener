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

# ── Styling ───────────────────────────────────────────────────────────────────
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

# Ensure all columns exist (backward compat with older reports)
_defaults = {
    'market_cap': None,
    'altman_score': None, 'altman_zone': None, 'altman_flag': False,
    'ar_ratio': None, 'dso': None, 'revenue_quality_flag': False,
    'non_op_ratio': None, 'earnings_quality_flag': False,
    'going_concern_flag': False,
    'auditor_name': None, 'big4_auditor': False, 'small_auditor_flag': False,
    'avg_volume_90d': None, 'volume_spike_ratio': None,
    'price_change_90d': None, 'illiquid_flag': False, 'pump_dump_flag': False,
    'net_insider_shares': None, 'insider_sale_count': None,
    'insider_buy_count': None, 'insider_selling_flag': False,
}
for col, default in _defaults.items():
    if col not in df.columns:
        df[col] = default

df['market_cap'] = pd.to_numeric(df['market_cap'], errors='coerce')

# ── Summary metrics placeholder (populated after filters) ─────────────────────
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
    "All sizes":         (0, 10_000_000),
    "Micro (<$300M)":    (0, 300),
    "Small ($150M-$1B)": (150, 1_000),
    "Mid ($1B-$10B)":    (1_000, 10_000),
    "Large (>$10B)":     (10_000, 10_000_000),
    "Custom":            None,
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
min_flags = st.sidebar.selectbox("Minimum Red Flags", [0, 1, 2, 3, 4, 5], index=0)
search = st.sidebar.text_input("Search by ticker or name", "")

st.sidebar.markdown("---")
st.sidebar.markdown("**Phase 2 signal filters**")
show_going_concern   = st.sidebar.checkbox("Going concern only", value=False)
show_pump_dump       = st.sidebar.checkbox("Pump & dump only", value=False)
show_insider_selling = st.sidebar.checkbox("Insider selling only", value=False)
show_altman_distress = st.sidebar.checkbox("Altman distress zone only", value=False)
show_small_auditor   = st.sidebar.checkbox("Small auditor only", value=False)

st.sidebar.markdown("---")
st.sidebar.markdown("**Score guide:**")
st.sidebar.markdown("- 🔴 70–100: High Risk")
st.sidebar.markdown("- 🟡 45–69: Medium Risk")
st.sidebar.markdown("- 🟢 0–44: Low Risk")

# ── Apply filters ─────────────────────────────────────────────────────────────
filtered = df.copy()

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

if show_going_concern:
    filtered = filtered[filtered['going_concern_flag'] == True]

if show_pump_dump:
    filtered = filtered[filtered['pump_dump_flag'] == True]

if show_insider_selling:
    filtered = filtered[filtered['insider_selling_flag'] == True]

if show_altman_distress:
    filtered = filtered[filtered['altman_flag'] == True]

if show_small_auditor:
    filtered = filtered[filtered['small_auditor_flag'] == True]

# ── Summary metrics (reflect current filters) ─────────────────────────────────
with METRICS_PLACEHOLDER.container():
    st.markdown("---")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Companies", len(filtered))
    c2.metric("🔴 High Risk",   len(filtered[filtered['risk'] == 'HIGH RISK']))
    c3.metric("🟡 Medium Risk", len(filtered[filtered['risk'] == 'MEDIUM RISK']))
    c4.metric("🟢 Low Risk",    len(filtered[filtered['risk'] == 'LOW RISK']))
    st.markdown("---")

st.markdown(f"**Showing {len(filtered)} companies**")

# ── Helpers ───────────────────────────────────────────────────────────────────
def fmt_market_cap(val):
    if pd.isna(val):
        return 'N/A'
    if val >= 1_000_000_000:
        return f"${val/1_000_000_000:.1f}B"
    return f"${val/1_000_000:.0f}M"


def fmt_volume(val):
    if pd.isna(val) or val is None:
        return 'N/A'
    if val >= 1_000_000:
        return f"{val/1_000_000:.1f}M"
    if val >= 1_000:
        return f"{val/1_000:.0f}K"
    return str(int(val))


# ── Main table ────────────────────────────────────────────────────────────────

# Build flag icon columns — 🔴 if flagged, blank if clean
def flags_icon(row):
    icons = []
    if row.get('going_concern_flag'):  icons.append('💀')
    if row.get('pump_dump_flag'):      icons.append('📈')
    if row.get('insider_selling_flag'): icons.append('👤')
    if row.get('small_auditor_flag'):  icons.append('🏢')
    return ' '.join(icons) if icons else '—'

filtered = filtered.copy()
filtered['Signals'] = filtered.apply(flags_icon, axis=1)

display_cols = {
    'ticker':          'Ticker',
    'risk':            'Risk',
    'fraud_score':     'Score',
    'red_flags_count': 'Flags',
    'market_cap':      'Mkt Cap',
    'beneish_score':   'Beneish',
    'piotroski_score': 'Piotroski',
    'altman_score':    'Altman Z',
    'accruals_ratio':  'Accruals',
    'cfd_ratio':       'CF Div.',
    'Signals':         'Signals',
    'name':            'Company',
}

table_df = filtered[list(display_cols.keys())].rename(columns=display_cols).copy()
table_df['Score']     = table_df['Score'].round(1)
table_df['Beneish']   = table_df['Beneish'].round(2)
table_df['Altman Z']  = table_df['Altman Z'].round(2)
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


def color_altman(val):
    if pd.isna(val):
        return ''
    if val < 1.81:
        return 'background-color: #5c1010'
    if val < 2.99:
        return 'background-color: #5c3a10'
    return ''


styled = (
    table_df.style
    .map(color_risk,   subset=['Risk'])
    .map(color_score,  subset=['Score'])
    .map(color_altman, subset=['Altman Z'])
    .format({
        'Score':    '{:.1f}',
        'Beneish':  '{:.2f}',
        'Altman Z': '{:.2f}',
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

    # ── Phase 1 signals ───────────────────────────────────────────────────────
    st.markdown("#### Phase 1 — Earnings & Cash Flow")
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

    # ── Phase 2 — Distress & Quality signals ──────────────────────────────────
    st.markdown("#### Phase 2 — Distress & Quality")
    p1, p2, p3, p4 = st.columns(4)

    # Altman Z-Score zone label
    altman_zone = row.get('altman_zone') or 'N/A'
    altman_flag_icon = "🔴" if row.get('altman_flag') else ("🟡" if altman_zone == 'grey' else "🟢")
    altman_val = f"{row['altman_score']:.2f} ({altman_zone})" if pd.notna(row.get('altman_score')) else "N/A"

    p1.metric(f"{altman_flag_icon} Altman Z-Score",
              altman_val,
              help="<1.81 = distress zone, 1.81-2.99 = grey, >2.99 = safe")

    rev_flag_icon = "🔴" if row.get('revenue_quality_flag') else "🟢"
    dso_val = f"{row['dso']:.0f}d" if pd.notna(row.get('dso')) else "N/A"
    ar_val  = f"{row['ar_ratio']:.3f}" if pd.notna(row.get('ar_ratio')) else "N/A"
    p2.metric(f"{rev_flag_icon} Revenue Quality",
              f"AR={ar_val} DSO={dso_val}",
              help="AR Ratio >0.25 or DSO >90 days = revenue may not be real cash")

    earn_flag_icon = "🔴" if row.get('earnings_quality_flag') else "🟢"
    non_op = row.get('non_op_ratio')
    p3.metric(f"{earn_flag_icon} Earnings Quality",
              f"{non_op:.3f}" if pd.notna(non_op) else "N/A",
              help="Non-operating income ratio >0.30 = earnings boosted by one-time gains")

    gc_flag_icon = "🔴" if row.get('going_concern_flag') else "🟢"
    p4.metric(f"{gc_flag_icon} Going Concern",
              "FLAGGED" if row.get('going_concern_flag') else "Clean",
              help="Company disclosed substantial doubt about ability to continue as a going concern")

    # ── Phase 2 — Market signals ───────────────────────────────────────────────
    st.markdown("#### Phase 2 — Market Signals")
    m1, m2, m3, m4 = st.columns(4)

    illiquid_icon = "🔴" if row.get('illiquid_flag') else "🟢"
    m1.metric(f"{illiquid_icon} Avg Volume (90d)",
              fmt_volume(row.get('avg_volume_90d')),
              help="Below 10,000 shares/day = illiquid, easier to manipulate")

    spike = row.get('volume_spike_ratio')
    spike_icon = "🔴" if row.get('pump_dump_flag') else ("🟡" if spike and spike > 2 else "🟢")
    m2.metric(f"{spike_icon} Volume Spike Ratio",
              f"{spike:.2f}x" if pd.notna(spike) else "N/A",
              help="Recent 30d volume / 90d average. >3x = abnormal spike")

    price_chg = row.get('price_change_90d')
    price_icon = "🔴" if (price_chg is not None and price_chg > 0.5) else "🟢"
    m3.metric(f"{price_icon} Price Change (90d)",
              f"{price_chg*100:.1f}%" if pd.notna(price_chg) else "N/A",
              help="Price change over 90 days. >50% combined with volume spike = pump & dump risk")

    pd_icon = "🔴" if row.get('pump_dump_flag') else "🟢"
    m4.metric(f"{pd_icon} Pump & Dump",
              "FLAGGED" if row.get('pump_dump_flag') else "Clean",
              help="Volume >3x spike AND price up >50% in 30 days = pump & dump pattern")

    # ── Phase 2 — Insider signals ──────────────────────────────────────────────
    st.markdown("#### Phase 2 — Insider Trading")
    i1, i2, i3, i4 = st.columns(4)

    ins_flag_icon = "🔴" if row.get('insider_selling_flag') else "🟢"
    i1.metric(f"{ins_flag_icon} Insider Selling",
              "FLAGGED" if row.get('insider_selling_flag') else "Clean",
              help="Net insider selling >10,000 shares with more sales than purchases in last 12 months")

    net_shares = row.get('net_insider_shares')
    i2.metric("Net Insider Shares",
              f"{int(net_shares):,}" if pd.notna(net_shares) else "N/A",
              help="Positive = net buying, Negative = net selling")

    sale_cnt = row.get('insider_sale_count')
    i3.metric("Sale Transactions",
              f"{int(sale_cnt)}" if pd.notna(sale_cnt) else "N/A",
              help="Number of open-market insider sale transactions (Form 4, last 12 months)")

    buy_cnt = row.get('insider_buy_count')
    i4.metric("Buy Transactions",
              f"{int(buy_cnt)}" if pd.notna(buy_cnt) else "N/A",
              help="Number of open-market insider purchase transactions (Form 4, last 12 months)")

    # ── Phase 2 — Auditor & Governance ────────────────────────────────────────
    st.markdown("#### Phase 2 — Auditor & Governance")
    a1, a2, a3, a4 = st.columns(4)

    auditor_name = row.get('auditor_name') or None
    is_big4      = row.get('big4_auditor', False)
    small_flag   = row.get('small_auditor_flag', False)
    gc_flag      = row.get('going_concern_flag', False)

    if auditor_name:
        auditor_quality_icon = "🟢" if is_big4 else ("🔴" if small_flag else "🟡")
        auditor_label        = "Big 4" if is_big4 else ("Small firm ⚠️" if small_flag else "Mid-tier")
    else:
        auditor_quality_icon = "⚫"
        auditor_label        = "Not available"
    a1.metric(f"{auditor_quality_icon} Auditor",
              auditor_label,
              help="Big 4 (Deloitte, EY, KPMG, PwC) provide the most rigorous audits. Small/unknown auditor on a large company is a red flag.")

    a2.metric("Auditor Name",
              (auditor_name[:30] if auditor_name else "N/A — requires premium data"),
              help="Registered public accounting firm. Not available via free EDGAR API; will be added in a future phase.")

    gc_icon = "🔴" if gc_flag else "🟢"
    a3.metric(f"{gc_icon} Going Concern",
              "FLAGGED" if gc_flag else "Clean",
              help="Company has formally disclosed substantial doubt about its ability to continue operating")

    a4.metric("Exchange",
              row.get('exchange') or 'N/A',
              help="Stock exchange where the company is listed")

    # ── Signal explanations ────────────────────────────────────────────────────
    st.markdown("#### What the signals mean")
    explanations = []

    # Phase 1
    if row.get('beneish_flag'):
        explanations.append("⚠️ **Beneish flagged**: Earnings may be manipulated — revenue inflation or expense deferral suspected.")
    if row.get('piotroski_weak'):
        explanations.append("⚠️ **Piotroski weak**: Poor financial health — low profitability, high leverage, or deteriorating operations.")
    if row.get('accruals_flag'):
        explanations.append("⚠️ **High accruals**: Reported profit is not being converted to cash — classic early fraud warning sign.")
    if row.get('cfd_flag'):
        explanations.append("⚠️ **Cash flow divergence**: Net income is significantly higher than operating cash flow — unsustainable.")

    # Phase 2
    if row.get('altman_flag'):
        explanations.append("⚠️ **Altman distress zone**: Z-Score below 1.81 — company shows financial distress patterns associated with bankruptcy and fraud.")
    elif row.get('altman_zone') == 'grey':
        explanations.append("🟡 **Altman grey zone**: Z-Score between 1.81-2.99 — borderline financial health, monitor closely.")
    if row.get('revenue_quality_flag'):
        explanations.append("⚠️ **Revenue quality risk**: High receivables relative to revenue — sales may not be converting to cash or revenue is being booked prematurely.")
    if row.get('earnings_quality_flag'):
        explanations.append("⚠️ **Low earnings quality**: More than 30% of net income comes from non-operating sources (asset sales, tax gains) — core business may be weaker than reported.")
    if row.get('pump_dump_flag'):
        explanations.append("⚠️ **Pump & dump pattern**: Abnormal volume spike combined with rapid price increase — possible market manipulation.")
    if row.get('illiquid_flag'):
        explanations.append("⚠️ **Illiquid stock**: Very low average trading volume — thin markets are vulnerable to price manipulation.")
    if row.get('insider_selling_flag'):
        explanations.append("⚠️ **Insider net selling**: Company insiders have been net sellers in the last 12 months — they may have information the market doesn't.")
    if row.get('small_auditor_flag'):
        explanations.append("⚠️ **Small auditor**: This company uses an unknown or small auditing firm despite its market cap. Fraudulent companies often select auditors unlikely to challenge questionable accounting.")
    if row.get('going_concern_flag'):
        explanations.append("🚨 **Going concern disclosed**: Company has formally disclosed doubt about its ability to continue operating — severe red flag.")

    if not explanations:
        explanations.append("✅ No major red flags detected for this company.")

    for e in explanations:
        st.markdown(e)
