from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.scoring import score_companies

# --- Case study definitions ---
# Each entry: ticker (as filed in dataset), market, fraud_year, fraud_type, narrative
_CASES: list[dict] = [
    {
        'name':       'Wirecard AG',
        'ticker':     'WDI',
        'market':     'DE',
        'fraud_year': 2020,
        'type':       'Accounting Manipulation — Missing Cash',
        'summary': (
            'Wirecard reported €1.9B held in trust accounts in the Philippines that did not exist. '
            'Auditor EY signed off for years. The collapse in June 2020 wiped €12B+ in market cap '
            'within days. It remains the largest post-war German corporate fraud.'
        ),
        'signals': [
            ('Days Sales Outstanding', 'DSO grew from 58 days (2015) to 97 days (2019) — receivables growing far faster than revenue.'),
            ('Receivables Accrual Index (DSRI)', 'DSRI > 1.5 by 2018, a strong Beneish manipulation flag.'),
            ('Asset Quality Index (AQI)', 'AQI > 1.3 — rapid growth in non-current assets relative to total assets.'),
            ('Cash Flow vs Net Income', 'Operating cash flow persistently below reported net income — classic accrual divergence.'),
            ('Auditor', 'EY issued clean opinions for nine years while KPMG special review (2020) flagged the gap.'),
        ],
        'signal_cols': ['dsri', 'aqi', 'sgai', 'beneish_m_score', 'sloan_accruals'],
    },
    {
        'name':       'Luckin Coffee Inc.',
        'ticker':     'LK',
        'market':     'US',
        'fraud_year': 2020,
        'type':       'Revenue Inflation — Fabricated Transactions',
        'summary': (
            'Luckin fabricated ~RMB 2.2B (~$310M) in sales in 2019 by creating phantom customer '
            'transactions through affiliated entities. The fraud was uncovered by a Muddy Waters '
            'short report in January 2020 and confirmed by internal audit in April 2020. '
            'Shares fell 75% in one session; Nasdaq delisted the stock in June 2020.'
        ),
        'signals': [
            ('Gross Margin Index (GMI)', 'GMI > 1.6 in FY2019 — gross margins were declining despite reported revenue growth.'),
            ('Sales Growth Index (SGI)', 'SGI of 4.6 in FY2019 — revenue growth far above sector peers.'),
            ('Total Accruals to Total Assets', 'Accruals ratio among the highest in the restaurant sector peer group.'),
            ('Revenue per store', 'Reported per-store revenue implausibly high versus foot-traffic data.'),
            ('Short interest', 'Short float exceeded 25% before the fraud announcement — market was skeptical.'),
        ],
        'signal_cols': ['gmi', 'sgi', 'sgai', 'beneish_m_score', 'revenue_growth'],
    },
    {
        'name':       'Enron Corporation',
        'ticker':     'ENE',
        'market':     'US',
        'fraud_year': 2001,
        'type':       'SPV Abuse — Off-Balance Sheet Debt',
        'summary': (
            'Enron used hundreds of Special Purpose Entities (SPEs) to hide debt and inflate profits. '
            'Executives used mark-to-market accounting on long-term contracts to book future revenue '
            'immediately. The $63B bankruptcy in December 2001 was the largest in US history at the time. '
            'Arthur Andersen, its auditor, was indicted and dissolved.'
        ),
        'signals': [
            ('Return on Assets trend', 'ROA declined from 6.5% (1996) to 1.5% (2000) while EPS grew — earnings quality deteriorating.'),
            ('Leverage vs peers', 'Debt-to-equity was 50% higher than pipeline peers but hidden via SPE off-balance-sheet treatment.'),
            ('Beneish DSRI', 'Receivables grew 2× revenue in 2000 — classic DSRI elevation.'),
            ('Total Accruals / Assets (TATA)', 'TATA > 0.07 in FY2000 and FY2001 — top decile for accrual manipulation.'),
            ('Altman Z-Score', 'Z-score fell below 1.81 distress threshold by Q2 2001.'),
        ],
        'signal_cols': ['dsri', 'tata', 'beneish_m_score', 'altman_z_score', 'roa'],
    },
    {
        'name':       'WorldCom Inc.',
        'ticker':     'WCOM',
        'market':     'US',
        'fraud_year': 2002,
        'type':       'Expense Capitalization — $11B Fraud',
        'summary': (
            'WorldCom capitalized $3.8B in ordinary line costs as capital expenditures in 2001–2002, '
            'inflating earnings by the same amount. Total fraud eventually reached $11B. '
            'The July 2002 bankruptcy at $107B in assets displaced Enron as the largest US bankruptcy. '
            'CEO Bernie Ebbers received a 25-year sentence.'
        ),
        'signals': [
            ('CapEx / Revenue ratio', 'CapEx jumped from 12% to 19% of revenue in 2001 — anomalous in a contracting telecom market.'),
            ('Asset Quality Index (AQI)', 'AQI > 1.5 — non-current assets growing as expense lines were capitalised.'),
            ('Cash Flow from Operations', 'OCF fell sharply while net income was stable — the fundamental red flag.'),
            ('Gross Margin Index (GMI)', 'GMI rose sharply in a declining revenue environment — impossible without cost manipulation.'),
            ('Altman Z-Score', 'Z-score dropped below 1.0 in the 12 months before bankruptcy.'),
        ],
        'signal_cols': ['aqi', 'gmi', 'beneish_m_score', 'altman_z_score', 'sloan_accruals'],
    },
    {
        'name':       'NMC Health plc',
        'ticker':     'NMC',
        'market':     'US',
        'fraud_year': 2020,
        'type':       'Hidden Debt — $4B Undisclosed Liabilities',
        'summary': (
            'UAE-listed (London Stock Exchange) hospital group NMC Health was found to have hidden '
            '$4B+ in debt not appearing on official balance sheets. Muddy Waters published a short '
            'report in December 2019. The company collapsed into administration in April 2020. '
            'Auditor EY was under scrutiny again.'
        ),
        'signals': [
            ('Debt-to-EBITDA', 'Disclosed leverage appeared moderate at 3.5× but actual leverage was >8×.'),
            ('Accounts Payable Days', 'DPO expanded from 45 to 95 days — supplier financing masking cash pressure.'),
            ('Free Cash Flow Yield', 'FCF yield was 50% below peer median despite similar margins.'),
            ('Beneish DSRI', 'Elevated receivables relative to revenue growth in FY2018.'),
            ('Governance flags', 'Founder-controlled board, complex cross-ownership, supplier-customer overlaps.'),
        ],
        'signal_cols': ['dsri', 'beneish_m_score', 'altman_z_score', 'sloan_accruals'],
    },
    {
        'name':       'Steinhoff International',
        'ticker':     'SNHJ',
        'market':     'DE',
        'fraud_year': 2017,
        'type':       'Accounting Irregularities — Multi-Year Revenue Inflation',
        'summary': (
            'Steinhoff (JSE + Frankfurt) disclosed "accounting irregularities" in December 2017 '
            'which turned out to be €6.5B+ in fictitious profit booked over multiple years. '
            'The share price lost 95% of its value in two days. PwC audited for years without '
            'flagging the scheme. CEO Markus Jooste fled.'
        ),
        'signals': [
            ('Gross Margin Index (GMI)', 'Gross margins consistently 200–300 bps above sector peers — implausibly high for a discount retailer.'),
            ('Sales Growth Index (SGI)', 'Revenue grew 30%+ YoY while same-store sales were flat — acquisition-masked organic inflation.'),
            ('Goodwill / Total Assets', 'Goodwill exceeded 40% of total assets after a series of opaque acquisitions.'),
            ('EBITDA vs OCF divergence', 'EBITDA-to-OCF conversion ratio below 0.6 for three consecutive years.'),
            ('Beneish M-Score', 'M-score above -2.22 threshold in FY2016 and FY2017.'),
        ],
        'signal_cols': ['gmi', 'sgi', 'beneish_m_score', 'altman_z_score'],
    },
    {
        'name':       'Valeant Pharmaceuticals',
        'ticker':     'VRX',
        'market':     'US',
        'fraud_year': 2016,
        'type':       'Channel Stuffing + Price Gouging',
        'summary': (
            'Valeant used specialty pharmacy Philidor to stuff drug distribution channels and '
            'inflate revenue, while simultaneously raising drug prices 500–1000%. '
            'The stock fell 90% from peak to trough. SEC investigated channel-stuffing practices '
            'and revenue recognition with its specialty pharmacy network.'
        ),
        'signals': [
            ('Receivables Growth', 'Accounts receivable grew 3× faster than revenue in 2015 — classic channel stuffing signal.'),
            ('DSRI (Beneish)', 'DSRI of 1.87 in FY2015 — well above the 1.03 manipulation threshold.'),
            ('Debt Load', 'Net debt exceeded $30B vs EBITDA of $5B — leverage unsustainable at any normal interest rate.'),
            ('Organic Revenue Growth', 'Strip out acquisitions and price hikes: organic volume growth was negative.'),
            ('Altman Z-Score', 'Z-score below 1.81 by early 2016 — distress zone.'),
        ],
        'signal_cols': ['dsri', 'sgi', 'beneish_m_score', 'altman_z_score', 'sloan_accruals'],
    },
    {
        'name':       'Satyam Computer Services',
        'ticker':     'SAY',
        'market':     'US',
        'fraud_year': 2009,
        'type':       'Balance Sheet Fabrication — $1.5B Cash Hole',
        'summary': (
            'Satyam chairman Ramalinga Raju confessed in January 2009 to fabricating ₹50.4B (~$1.47B) '
            'in cash and bank balances that did not exist. Fake fixed deposits, inflated receivables, '
            'and understated liabilities had been on the books for years. The Indian IT outsourcing '
            'giant collapsed overnight in what became known as "India\'s Enron." '
            'PricewaterhouseCoopers India signed off on the accounts for eight years.'
        ),
        'signals': [
            ('Cash vs Operating Cash Flow', 'Reported cash balance was massive yet OCF was inconsistently low — cash on the balance sheet was fictitious.'),
            ('Receivables Growth (DSRI)', 'DSRI rose above 1.4 in FY2007–2008 — receivables growing faster than revenue, a classic Beneish flag.'),
            ('Return on Assets decline', 'ROA fell from 18% (2004) to 8% (2008) while reported margins stayed stable — earnings quality divergence.'),
            ('Accruals ratio (TATA)', 'Total accruals to total assets climbed into the top decile of Indian IT peers.'),
            ('Auditor Independence', 'PwC India had a 10-year tenure; local affiliate fees were disproportionately small, suggesting inadequate audit scope.'),
        ],
        'signal_cols': ['dsri', 'tata', 'beneish_m_score', 'altman_z_score', 'roa'],
    },
    {
        'name':       'Parmalat SpA',
        'ticker':     'PARME',
        'market':     'US',
        'fraud_year': 2003,
        'type':       'Phantom Cash — €14B Black Hole',
        'summary': (
            'Parmalat, the Italian dairy giant, collapsed in December 2003 after a €14B accounting '
            'hole was discovered. The company claimed €3.9B in a Bank of America account in the '
            'Cayman Islands — a document that proved to be a forgery. The fraud had been running '
            'for over a decade, funded by ever-increasing debt hidden in offshore subsidiaries. '
            'Grant Thornton and Deloitte both audited parts of the empire without detecting it.'
        ),
        'signals': [
            ('Debt-to-Equity explosion', 'Reported debt grew from €2B (1997) to €14B (2003) — a 7× increase hidden across 200+ subsidiaries.'),
            ('Cash vs Debt paradox', 'Claimed to hold €4B+ cash while simultaneously borrowing billions — a logical impossibility that went unchallenged.'),
            ('Asset Quality Index (AQI)', 'AQI > 1.4 — non-current assets at offshore entities ballooned without operational explanation.'),
            ('Interest Coverage ratio', 'Interest coverage fell below 1.0 by 2002 — debt was unpayable from operating income alone.'),
            ('Altman Z-Score', 'Z-score entered distress zone (< 1.81) by FY2001 — two years before the collapse.'),
        ],
        'signal_cols': ['aqi', 'beneish_m_score', 'altman_z_score', 'sloan_accruals'],
    },
    {
        'name':       'Nikola Corporation',
        'ticker':     'NKLA',
        'market':     'US',
        'fraud_year': 2020,
        'type':       'Technology Fabrication — Fake Demo / SEC Fraud',
        'summary': (
            'Nikola, an electric truck startup, was accused by Hindenburg Research in September 2020 '
            'of being "an intricate fraud" — most prominently staging a promotional video showing a '
            'truck driving under its own power when it had actually been pushed down a hill. '
            'Founder Trevor Milton resigned and was convicted of fraud in 2022. '
            'SEC and DOJ both brought charges. The stock fell over 80% from peak.'
        ),
        'signals': [
            ('Revenue vs Valuation', 'Market cap exceeded $30B with $0 in actual revenue — valuation entirely based on unverifiable technology claims.'),
            ('Negative OCF with large stock issuance', 'Operating cash flow deeply negative while share-based compensation was the primary "asset" — classic SPAC fraud profile.'),
            ('Sales Growth Index (SGI)', 'SGI from forecasted to actual revenue: effectively undefined — zero delivery milestone met.'),
            ('Insider selling', 'Founder sold $70M+ in shares before fraud revelation — stock-based dilution at the expense of retail investors.'),
            ('Fraud Score Composite', 'High governance and dilution sub-scores driven by SPAC structure, single founder control, no audited revenue history.'),
        ],
        'signal_cols': ['sgi', 'beneish_m_score', 'fraud_score_composite', 'fraud_score_dilution', 'fraud_score_governance'],
    },
    {
        'name':       'Theranos Inc.',
        'ticker':     'THRNOS',
        'market':     'US',
        'fraud_year': 2015,
        'type':       'Technology Fabrication — $9B Blood-Testing Fraud',
        'summary': (
            'Theranos, a Silicon Valley blood-testing startup, claimed its Edison device could run '
            'hundreds of tests from a single finger-prick drop of blood. In October 2015 a WSJ '
            'investigation revealed the technology did not work: most tests were run on conventional '
            'Siemens machines. Founder Elizabeth Holmes and president Ramesh Balwani were both '
            'convicted of fraud. The company, valued at $9B, had raised $900M+ from investors '
            'without sharing audited financials or allowing standard due diligence.'
        ),
        'signals': [
            ('Revenue vs Technology Claims', 'Claimed test cost of $15 vs industry standard $100+ — a claim that would require a verified breakthrough unsubstantiated by any peer-reviewed publication.'),
            ('Cash Burn vs Revenue', 'Raised $900M+ in equity but disclosed zero commercial revenue; valuation was entirely based on forward promises with no audited revenue base.'),
            ('Due Diligence Restriction', 'Investors were contractually barred from visiting labs or conducting standard technical diligence — a structural red flag in any private placement.'),
            ('Governance Concentration', 'Founder held 99%+ voting control via super-voting shares; board was celebrity-heavy but lacked medical diagnostics expertise entirely.'),
            ('Regulatory Non-Compliance', 'CLIA lab certification was obtained under a subsidiary brand; FDA clearance was never obtained for the Edison device despite patient results being used clinically.'),
        ],
        'signal_cols': ['fraud_score_governance', 'fraud_score_composite'],
    },
    {
        'name':       'Adelphia Communications',
        'ticker':     'ADELQ',
        'market':     'US',
        'fraud_year': 2002,
        'type':       'Related-Party Looting — $3.1B Off-Book Family Loans',
        'summary': (
            'Adelphia Communications, founded by the Rigas family, concealed $3.1B in off-balance-sheet '
            'co-borrowing arrangements that were used to fund personal expenses for the founding family — '
            'including a private golf course, a golf management company, and a personal lumber business. '
            'The fraud was disclosed in March 2002 and the company filed for bankruptcy in June 2002. '
            'John Rigas (founder) and his son Timothy both received prison sentences.'
        ),
        'signals': [
            ('Off-Balance-Sheet Liabilities', '$3.1B in co-borrowing arrangements for Rigas family entities not disclosed on Adelphia balance sheets — true leverage was 14× EBITDA.'),
            ('Related-Party Transactions', 'Founding family entities received company guarantees and cash advances for personal asset purchases; disclosure was minimal and buried in footnotes.'),
            ('Debt / EBITDA', 'Disclosed leverage of ~8× was already high; including undisclosed co-borrowings the true ratio exceeded 14× — structurally insolvent.'),
            ('OCF vs Reported Earnings', 'Free cash flow was deeply negative while reported net income was positive — EBITDA was the concealment metric of choice across disclosures.'),
            ('Altman Z-Score', 'Z-score fell below 1.81 in FY2001, two quarters before the June 2002 bankruptcy filing.'),
        ],
        'signal_cols': ['beneish_m_score', 'altman_z_score', 'sloan_accruals', 'aqi'],
    },
    {
        'name':       'HealthSouth Corporation',
        'ticker':     'HLSH',
        'market':     'US',
        'fraud_year': 2003,
        'type':       'Earnings Fabrication — $2.7B Inflated Net Income',
        'summary': (
            'HealthSouth CEO Richard Scrushy directed management to set quarterly earnings targets '
            'and then create fraudulent journal entries backward from the target — a practice that '
            'ran from 1996 to 2003 and inflated cumulative earnings by $2.7B. Over 2,700 false '
            'journal entries were discovered by forensic accountants. Ernst & Young signed off on '
            'the financials for 17 years. Fifteen HealthSouth executives pleaded guilty; Scrushy '
            'was acquitted of fraud but later convicted of bribery in a separate case.'
        ),
        'signals': [
            ('Earnings vs OCF Gap', 'EPS grew steadily while OCF consistently underperformed reported net income by 30–50% per year — the gap was filled entirely by fictitious asset additions.'),
            ('Asset Quality Index (AQI)', 'AQI > 1.6 — property & equipment overstated by capitalising items that were maintenance expense, a textbook Beneish AQI signal.'),
            ('Sloan Accruals', 'Accruals ratio in the top decile for healthcare services peers consistently from 1997 to 2003 — persistent elevated accruals are a reliable fraud precursor.'),
            ('Gross Margin Stability', 'Margins were suspiciously stable despite large swings in Medicare reimbursement rates — real healthcare operations exhibit margin volatility.'),
            ('Auditor Tenure Risk', 'Ernst & Young engagement ran for 17 years; long-tenured auditors develop familiarity bias, reducing professional skepticism on unusual journal entries.'),
        ],
        'signal_cols': ['aqi', 'sloan_accruals', 'beneish_m_score', 'altman_z_score'],
    },
    {
        'name':       'Autonomy Corporation',
        'ticker':     'AUTNF',
        'market':     'US',
        'fraud_year': 2012,
        'type':       'Revenue Inflation — $5B HP Acquisition Writedown',
        'summary': (
            'Hewlett-Packard acquired UK software company Autonomy for $11.1B in October 2011 and '
            'wrote down $8.8B just 13 months later, attributing $5B of the loss to "serious accounting '
            'improprieties" at Autonomy. Alleged techniques included misclassifying hardware revenue '
            'as software, round-trip transactions with resellers, and pulling forward revenue from '
            'multi-year contracts. Autonomy founder Mike Lynch was extradited to the US and acquitted '
            'in 2024 after a landmark trial.'
        ),
        'signals': [
            ('Revenue Misclassification', 'Low-margin hardware sales were recorded as high-margin software revenue, inflating reported gross margins by an estimated 10+ percentage points.'),
            ('Gross Margin Index (GMI)', 'Reported 80%+ gross margins were implausible for a business with known hardware distribution activity — GMI was elevated vs pure-play software peers.'),
            ('Round-Trip Transactions', 'Value-added resellers were allegedly paid to purchase Autonomy software and immediately resell it back, inflating current-period recognised revenue.'),
            ('DSRI (Beneish)', 'DSRI above 1.3 in FY2010 and FY2011 — receivables growing disproportionately faster than revenue growth.'),
            ('Revenue Growth vs Cash Collection', 'Revenue grew 60%+ YoY in 2010–2011 while DSO expanded from ~60 to ~110 days — accelerating revenue recognition without matching cash inflow.'),
        ],
        'signal_cols': ['dsri', 'gmi', 'beneish_m_score', 'sloan_accruals'],
    },
    {
        'name':       'Toshiba Corporation',
        'ticker':     'TOSBF',
        'market':     'JP',
        'fraud_year': 2015,
        'type':       'Earnings Fabrication — ¥152B Overstated Profit (7 Years)',
        'summary': (
            'Toshiba disclosed in April 2015 that it had overstated cumulative pre-tax profits by '
            '¥151.8B (~$1.2B) over seven fiscal years, spanning three consecutive CEO tenures. '
            'An independent panel found systematic "challenges" — targets set above business unit '
            'capacity — that created a cultural mandate to manipulate. Key techniques included '
            'percentage-of-completion inflation in infrastructure projects and component '
            'cost-recognition deferral in semiconductors. PricewaterhouseCoopers Japan issued a '
            'qualified opinion retroactively but had signed clean opinions for the fraudulent periods.'
        ),
        'signals': [
            ('Percentage-of-Completion Manipulation', 'Long-term infrastructure project revenue pulled forward via systematically overstated completion percentages — a technique invisible without project-level disclosure.'),
            ('Operating Income vs OCF Divergence', 'Operating income exceeded OCF by a cumulative ¥152B over 7 years — a slow drift that compounded from ¥5–10B/year to >¥50B/year by FY2014.'),
            ('Gross Margin Index (GMI)', 'Semiconductor and infrastructure division margins were 200–400 bps above peer benchmarks with no stated competitive explanation.'),
            ('Management Culture Indicators', 'Internal investigation cited "challenges" (management targets) consistently set above achievable capacity — a governance failure enabling systematic manipulation.'),
            ('Beneish M-Score Trajectory', 'M-score deteriorated from approximately −2.5 to −1.9 between FY2009 and FY2014, crossing the −2.22 manipulation zone threshold.'),
        ],
        'signal_cols': ['gmi', 'beneish_m_score', 'altman_z_score', 'sloan_accruals'],
    },
]

_SCORE_HISTORY_COLS = [
    'beneish_m_score', 'altman_z_score', 'piotroski_f_score',
    'fraud_score_composite', 'fraud_score_accounting', 'sloan_accruals',
    'ml_score_1y', 'ml_score_3y',
]


def _score_chart(company_df: pd.DataFrame, fraud_year: int) -> go.Figure:
    avail = [c for c in _SCORE_HISTORY_COLS if c in company_df.columns and company_df[c].notna().any()]
    fig = go.Figure()
    colors = ['#EF5350', '#42A5F5', '#66BB6A', '#FFA726', '#AB47BC', '#26C6DA']
    for i, col in enumerate(avail):
        data = company_df[['fiscal_year', col]].dropna()
        fig.add_trace(go.Scatter(
            x=data['fiscal_year'], y=data[col],
            mode='lines+markers',
            name=col.replace('_', ' ').title(),
            line=dict(color=colors[i % len(colors)], width=2),
            marker=dict(size=7),
        ))
    fig.add_vline(
        x=fraud_year, line_dash='dash', line_color='red',
        annotation_text='Fraud revealed', annotation_position='top left',
    )
    if 'beneish_m_score' in avail:
        fig.add_hline(y=-2.22, line_dash='dot', line_color='orange',
                      annotation_text='Beneish -2.22')
    fig.update_layout(
        height=340, margin=dict(t=20, b=20),
        xaxis_title='Fiscal Year', yaxis_title='Score',
        legend=dict(orientation='h', y=-0.3),
    )
    return fig


def tab_case_studies(df_all: pd.DataFrame, models: dict | None = None, meta: dict | None = None) -> None:
    st.title('📚 Fraud Case Study Library')
    st.caption(
        'Real-world fraud cases with the quantitative signals that were detectable in annual filings '
        'before the fraud was publicly revealed. Where the company exists in our dataset, live model '
        'scores are shown.'
    )

    case_names = [c['name'] for c in _CASES]
    selected_name = st.selectbox('Select case', case_names, key='cs_select')
    case = next(c for c in _CASES if c['name'] == selected_name)

    ticker     = case['ticker']
    market     = case['market']
    fraud_year = case['fraud_year']

    st.markdown('---')
    h1, h2 = st.columns([3, 1])
    with h1:
        st.subheader(f'{case["name"]}  ·  {ticker}  ·  {market}')
        st.markdown(f'**Fraud type:** {case["type"]}')
    with h2:
        st.metric('Fraud Revealed', str(fraud_year))

    st.markdown(case['summary'])

    # --- Pre-fraud signals ---
    st.markdown('---')
    st.subheader('🔍 Pre-Fraud Warning Signals')
    for i, (signal_name, signal_desc) in enumerate(case['signals']):
        with st.expander(f'{i+1}. {signal_name}', expanded=(i == 0)):
            st.markdown(signal_desc)

    # --- Live scores from dataset (if ticker found) ---
    st.markdown('---')
    ann = df_all[(df_all['ticker'] == ticker) & (df_all.get('period_type', pd.Series('annual', index=df_all.index)) == 'annual')].copy() if 'period_type' in df_all.columns else df_all[df_all['ticker'] == ticker].copy()

    if ann.empty and 'market' in df_all.columns:
        # try without market filter — ticker might exist under different market code
        ann = df_all[df_all['ticker'] == ticker].copy()

    if ann.empty:
        st.info(
            f'`{ticker}` is not in the current dataset '
            f'(company may have been delisted before our data coverage or uses a different ticker symbol). '
            f'The signals above are based on published research and academic literature.'
        )
    else:
        ann = ann.sort_values('fiscal_year')
        if models and meta:
            for h in ['1y', '3y']:
                scored = score_companies(ann, models, meta, horizon=h)
                if 'ml_score' in scored.columns:
                    ann[f'ml_score_{h}'] = scored['ml_score'].values
        st.subheader(f'📊 Model Scores from Dataset — {ticker}')
        st.caption(f'{len(ann)} annual rows found | Fiscal years: {int(ann["fiscal_year"].min())}–{int(ann["fiscal_year"].max())}')

        pre_fraud = ann[ann['fiscal_year'] < fraud_year]
        if not pre_fraud.empty:
            # Metrics from the last pre-fraud year
            last_pre = pre_fraud.iloc[-1]
            m1, m2, m3, m4 = st.columns(4)
            bm = last_pre.get('beneish_m_score')
            az = last_pre.get('altman_z_score')
            fsc = last_pre.get('fraud_score_composite')
            pf = last_pre.get('piotroski_f_score')
            m1.metric('Beneish M (last pre-fraud)',
                      f'{float(bm):.2f}' if pd.notna(bm) else '—',
                      delta='⚠️ manipulator' if pd.notna(bm) and bm > -2.22 else '✅ safe',
                      delta_color='inverse' if pd.notna(bm) and bm > -2.22 else 'off')
            m2.metric('Altman Z (last pre-fraud)',
                      f'{float(az):.2f}' if pd.notna(az) else '—',
                      delta='distress' if pd.notna(az) and az < 1.81 else 'safe')
            m3.metric('Fraud Score (last pre-fraud)',
                      f'{float(fsc):.3f}' if pd.notna(fsc) else '—',
                      delta='🔴 High risk' if pd.notna(fsc) and fsc > 0.65 else
                            '🟠 Medium' if pd.notna(fsc) and fsc > 0.35 else '🟢 Low risk')
            m4.metric('Piotroski F (last pre-fraud)',
                      f'{int(pf)}/9' if pd.notna(pf) else '—')

        # Score timeline chart
        avail_score_cols = [c for c in _SCORE_HISTORY_COLS if c in ann.columns and ann[c].notna().any()]
        if avail_score_cols:
            chart_fig = _score_chart(ann, fraud_year)
            st.plotly_chart(chart_fig, use_container_width=True)

        # Raw scores table
        with st.expander('📋 Full score history', expanded=False):
            show_cols = ['fiscal_year'] + [c for c in _SCORE_HISTORY_COLS if c in ann.columns]
            score_tbl = ann[show_cols].copy()
            for col in score_tbl.select_dtypes('float').columns:
                score_tbl[col] = score_tbl[col].map(lambda x: f'{x:.4f}' if pd.notna(x) else '—')
            st.dataframe(score_tbl, use_container_width=True, hide_index=True)

    # --- Key lesson ---
    st.markdown('---')
    st.subheader('📝 Key Lesson')
    lesson_map = {
        'Wirecard AG':              'A company generating revenue almost entirely through third-party acquirers and storing cash in trust accounts in opaque jurisdictions should trigger maximum scrutiny of every cash-flow statement. Beneish DSRI and OCF/NI divergence were both elevated 2+ years before collapse.',
        'Luckin Coffee Inc.':       'Hyper-growth combined with opaque related-party distribution channels is a classic pump scheme. The SGI of 4.6× peers and rapidly worsening GMI were measurable signals well before the short seller report.',
        'Enron Corporation':        "When earnings grow but ROA falls, ask where the return is going. Enron hid debt in SPEs that don't appear on consolidated balance sheets — but the OCF/NI divergence was visible every year from 1998 onwards.",
        'WorldCom Inc.':            'Capitalizing operating costs is the oldest fraud in the book. A sudden, unexplained jump in CapEx/Revenue during a revenue decline is the primary signal. AQI > 1.5 flagged it 18 months before the restatement.',
        'NMC Health plc':           'Companies with complex cross-ownership, high supplier concentration, and expanding accounts payable should be treated as high governance risk regardless of clean audit opinions. FCF/EBITDA ratio below 0.5 is a clear red flag.',
        'Steinhoff International':  'Goodwill-heavy acquisition strategies combined with implausibly stable margins are fertile ground for fraud. The GMI and SGI signals were anomalous relative to discount retail peers 3 years before the collapse.',
        'Valeant Pharmaceuticals':  'Channel stuffing shows up first in the DSRI. When receivables grow faster than revenue in a pharmaceutical company, and the distribution network includes captive specialty pharmacies, the revenue number is unreliable.',
        'Satyam Computer Services': 'When a cash-rich company consistently generates weak operating cash flow, the cash is almost certainly fictional. DSRI elevation combined with ROA deterioration is the earliest reliable signal in balance-sheet fabrication frauds.',
        'Parmalat SpA':             'Claimed cash balances of €4B+ alongside €14B in debt should have been impossible — the interest payments alone exceeded reported operating income. Z-score distress and AQI elevation were detectable years before the forged bank document surfaced.',
        'Nikola Corporation':       'SPAC-structure companies with no revenue history, single-founder governance, and stock-based dilution as the primary cash mechanism warrant maximum skepticism on every technology claim. Governance and dilution sub-scores catch this class of fraud before any accounting manipulation is visible.',
        'Theranos Inc.':            'When a private company refuses audited financials, bars investors from standard diligence, and makes technology claims unsupported by peer-reviewed evidence, the governance and dilution red flags are sufficient to reject the investment before any accounting data is available. Voting control concentration is the single fastest signal.',
        'Adelphia Communications':  'Related-party looting is the hardest fraud to detect from public filings alone, but the leverage signal is always present. A disclosed debt/EBITDA of 8× in a capital-intensive cable business should have triggered scrutiny of off-balance-sheet guarantees. Altman Z below 1.81 preceded bankruptcy by two quarters.',
        'HealthSouth Corporation':  '"Earnings first, entries second" manipulation produces a consistent pattern: rising EPS with rising accruals and stagnant OCF. Sloan accruals in the top peer-decile for 7 consecutive years is an independently sufficient reason to exit the position regardless of the income statement story.',
        'Autonomy Corporation':     'Gross margins above 80% in a business with known hardware activity are a contradiction. The GMI and DSRI signals were both elevated in the two years before the HP acquisition — a buyer conducting standard Beneish analysis should have flagged the revenue quality before signing the purchase agreement.',
        'Toshiba Corporation':      'POC-based revenue manipulation is gradual — ¥5–10B/year for the first few years, then accelerating. The OCF/operating-income divergence compounded over seven years before anyone acted. Monitoring this ratio annually with a simple threshold (< 0.80 conversion) would have flagged Toshiba in FY2011 — four years before the restatement.',
    }
    st.info(lesson_map.get(case['name'], 'Study the pre-fraud signals carefully — they were present.'))

    # --- Browse all cases summary ---
    st.markdown('---')
    with st.expander('📖 All Cases Overview', expanded=False):
        rows = []
        for c in _CASES:
            tk = c['ticker']
            in_ds = (df_all['ticker'] == tk).any() if 'ticker' in df_all.columns else False
            rows.append({
                'Company':    c['name'],
                'Ticker':     tk,
                'Market':     c['market'],
                'Year':       c['fraud_year'],
                'Type':       c['type'].split('—')[0].strip(),
                'In Dataset': '✅' if in_ds else '❌',
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
