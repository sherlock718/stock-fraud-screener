from __future__ import annotations

_FEATURE_GROUPS: dict[str, list[str]] = {
    '🚨 Fraud Signals': [
        'beneish_m_score', 'altman_z_score', 'piotroski_f_score',
        'sloan_accruals', 'accruals_to_assets', 'wc_accruals_to_assets',
        'beneish_m_score_sector_pct', 'altman_z_score_sector_pct', 'sloan_accruals_sector_pct',
    ],
    '📊 Beneish Components': [
        'beneish_dsri', 'beneish_gmi', 'beneish_aqi', 'beneish_sgi',
        'beneish_depi', 'beneish_sgai', 'beneish_lvgi', 'beneish_tata',
    ],
    '💉 Dilution & Shares': [
        'shares_dilution', 'shares_growth', 'eps_diluted', 'shares_outstanding',
        'common_shares_outstanding', 'dividends_per_share',
    ],
    '💰 Valuation': [
        'pe_ratio', 'pb_ratio', 'ps_ratio', 'ev_ebitda', 'ev_revenue', 'ev_ocf',
        'earnings_yield', 'fcf_yield', 'value_composite', 'pe_ratio_sector_pct',
    ],
    '⚙️ Quality': [
        'roa', 'roe', 'roic', 'gross_margin', 'operating_margin', 'net_margin',
        'current_ratio', 'net_debt_to_equity', 'quality_composite', 'accruals_avg_3y',
    ],
    '📈 Momentum & Growth': [
        'revenue_growth_yoy', 'eps_growth_yoy', 'ocf_growth_yoy', 'debt_growth_yoy',
        'roa_trend_3y', 'gross_margin_trend_3y', 'revenue_cagr_3y',
        'momentum_12m_prior', 'momentum_6m_prior', 'momentum_3m_prior',
    ],
    '🏗️ Balance Sheet': [
        'total_assets', 'total_debt', 'long_term_debt', 'short_term_debt',
        'cash', 'equity', 'retained_earnings', 'receivables', 'inventory',
        'current_assets', 'current_liabilities', 'operating_cash_flow',
    ],
}

_BENEISH_LABELS: dict[str, str] = {
    'beneish_dsri':  'DSRI (Receivables)',
    'beneish_gmi':   'GMI (Gross Margin)',
    'beneish_aqi':   'AQI (Asset Quality)',
    'beneish_sgi':   'SGI (Sales Growth)',
    'beneish_depi':  'DEPI (Depreciation)',
    'beneish_sgai':  'SGAI (SGA)',
    'beneish_lvgi':  'LVGI (Leverage)',
    'beneish_tata':  'TATA (Accruals)',
}

_FEATURE_DESCRIPTIONS: dict[str, dict] = {
    'beneish_m_score':       {'label': 'Beneish M-Score',            'high_is': 'bad',     'fmt': '.2f',  'desc': 'Composite manipulation score. Values above -2.22 are a manipulation warning.'},
    'altman_z_score':        {'label': 'Altman Z-Score',             'high_is': 'good',    'fmt': '.2f',  'desc': 'Solvency score. Above 2.99 is safe; below 1.81 is distress zone.'},
    'piotroski_f_score':     {'label': 'Piotroski F-Score',          'high_is': 'good',    'fmt': '.0f',  'desc': '9-point quality test across profitability, leverage, and efficiency. Scores ≥ 6 indicate strong fundamentals.'},
    'sloan_accruals':        {'label': 'Sloan Accruals',             'high_is': 'bad',     'fmt': '.4f',  'desc': 'Difference between accounting income and cash income. Positive values signal earnings not backed by cash.'},
    'accruals_to_assets':    {'label': 'Accruals / Total Assets',    'high_is': 'bad',     'fmt': '.4f',  'desc': '(Net income − operating cash flow) / total assets. Values far above zero indicate low earnings quality.'},
    'wc_accruals_to_assets': {'label': 'Working Capital Accruals',   'high_is': 'bad',     'fmt': '.4f',  'desc': 'Change in non-cash working capital relative to assets. Persistent build-up may indicate earnings management.'},
    'accruals_avg_3y':       {'label': 'Avg Accruals (3-Year)',      'high_is': 'bad',     'fmt': '.4f',  'desc': 'Three-year average accruals ratio. A sustained pattern of high accruals is a stronger signal than a single-year spike.'},
    'beneish_dsri':          {'label': 'DSRI — Receivables Index',   'high_is': 'bad',     'fmt': '.3f',  'desc': 'Receivables-to-sales ratio vs prior year. Values above 1.0 mean receivables are growing faster than revenue — a classic revenue recognition warning.'},
    'beneish_gmi':           {'label': 'GMI — Gross Margin Index',   'high_is': 'bad',     'fmt': '.3f',  'desc': 'Prior gross margin / current gross margin. Above 1.0 signals deteriorating margins, which can create incentive to manipulate earnings.'},
    'beneish_aqi':           {'label': 'AQI — Asset Quality Index',  'high_is': 'bad',     'fmt': '.3f',  'desc': 'Ratio of non-current, non-physical assets. Values above 1.0 indicate increasing capitalisation of expenses.'},
    'beneish_sgi':           {'label': 'SGI — Sales Growth Index',   'high_is': 'bad',     'fmt': '.3f',  'desc': 'Current revenue / prior revenue. High growth can increase pressure to maintain momentum through manipulation.'},
    'beneish_depi':          {'label': 'DEPI — Depreciation Index',  'high_is': 'bad',     'fmt': '.3f',  'desc': 'Change in depreciation rate. Above 1.0 suggests a slowing depreciation policy, which inflates reported assets.'},
    'beneish_sgai':          {'label': 'SGAI — SGA Expense Index',   'high_is': 'bad',     'fmt': '.3f',  'desc': 'SG&A expenses relative to revenue vs prior year. Rising overhead relative to sales is a warning sign.'},
    'beneish_lvgi':          {'label': 'LVGI — Leverage Index',      'high_is': 'bad',     'fmt': '.3f',  'desc': 'Change in total leverage. Rising debt increases pressure to meet earnings covenants.'},
    'beneish_tata':          {'label': 'TATA — Total Accruals',      'high_is': 'bad',     'fmt': '.3f',  'desc': 'Total accruals to total assets — similar to Sloan accruals within the Beneish framework.'},
    'roa':                   {'label': 'Return on Assets',           'high_is': 'good',    'fmt': '.3f',  'desc': 'Net income as a share of total assets. Higher values indicate efficient asset use.'},
    'roe':                   {'label': 'Return on Equity',           'high_is': 'good',    'fmt': '.3f',  'desc': 'Net income relative to shareholder equity. High ROE signals strong earnings power.'},
    'roic':                  {'label': 'Return on Invested Capital', 'high_is': 'good',    'fmt': '.3f',  'desc': 'After-tax operating profit relative to invested capital. Strong ROIC indicates a defensible business.'},
    'gross_margin':          {'label': 'Gross Margin',               'high_is': 'good',    'fmt': '.3f',  'desc': 'Revenue minus COGS as a % of revenue. Stable or expanding margin is a quality signal.'},
    'operating_margin':      {'label': 'Operating Margin',           'high_is': 'good',    'fmt': '.3f',  'desc': 'Operating income as a % of revenue — core profitability before interest and taxes.'},
    'net_margin':            {'label': 'Net Profit Margin',          'high_is': 'good',    'fmt': '.3f',  'desc': 'Net income as a % of revenue after all expenses.'},
    'current_ratio':         {'label': 'Current Ratio',              'high_is': 'good',    'fmt': '.2f',  'desc': 'Current assets / current liabilities. Below 1.0 signals near-term liquidity stress.'},
    'net_debt_to_equity':    {'label': 'Net Debt / Equity',          'high_is': 'bad',     'fmt': '.2f',  'desc': '(Total debt − cash) / equity. High values indicate financial leverage risk.'},
    'fcf_to_ni':             {'label': 'FCF / Net Income',           'high_is': 'good',    'fmt': '.3f',  'desc': 'Free cash flow relative to reported net income. Values near or above 1.0 confirm that earnings are backed by real cash.'},
    'fcf_yield':             {'label': 'FCF Yield',                  'high_is': 'good',    'fmt': '.3f',  'desc': 'Free cash flow relative to market cap. A high FCF yield indicates the company generates real cash relative to its price.'},
    'revenue_growth_yoy':    {'label': 'Revenue Growth (YoY)',       'high_is': 'neutral', 'fmt': '.3f',  'desc': 'Year-on-year revenue change. Very rapid growth can increase the incentive to sustain momentum through manipulation.'},
    'eps_growth_yoy':        {'label': 'EPS Growth (YoY)',           'high_is': 'good',    'fmt': '.3f',  'desc': 'Year-on-year earnings-per-share growth.'},
    'debt_growth_yoy':       {'label': 'Debt Growth (YoY)',          'high_is': 'bad',     'fmt': '.3f',  'desc': 'Year-on-year growth in total debt. Rapid debt growth alongside weak cash flow is a risk factor.'},
    'roa_trend_3y':          {'label': 'ROA Trend (3-Year)',         'high_is': 'good',    'fmt': '.4f',  'desc': 'Slope of ROA over three years. A positive trend signals a strengthening business.'},
    'gross_margin_trend_3y': {'label': 'Gross Margin Trend (3-Year)','high_is': 'good',    'fmt': '.4f',  'desc': 'Slope of gross margin over three years. Declining margins can motivate aggressive accounting.'},
    'revenue_cagr_3y':       {'label': 'Revenue CAGR (3-Year)',      'high_is': 'good',    'fmt': '.3f',  'desc': 'Compound annual revenue growth over three years.'},
    'momentum_12m_prior':    {'label': '12-Month Price Momentum',    'high_is': 'neutral', 'fmt': '.3f',  'desc': 'Stock return over the prior 12 months (excluding last month).'},
    'pe_ratio':              {'label': 'P/E Ratio',                  'high_is': 'neutral', 'fmt': '.1f',  'desc': 'Price relative to earnings. An extremely high P/E sets expectations that can be hard to meet honestly.'},
    'pb_ratio':              {'label': 'P/B Ratio',                  'high_is': 'neutral', 'fmt': '.2f',  'desc': 'Market value relative to book value.'},
    'ev_ebitda':             {'label': 'EV / EBITDA',                'high_is': 'neutral', 'fmt': '.1f',  'desc': 'Enterprise value relative to operating earnings.'},
    'earnings_yield':        {'label': 'Earnings Yield',             'high_is': 'good',    'fmt': '.3f',  'desc': 'Inverse of P/E. A higher earnings yield signals potential value.'},
    'shares_dilution':       {'label': 'Share Dilution',             'high_is': 'bad',     'fmt': '.4f',  'desc': 'Change in diluted share count. Persistent dilution transfers value away from existing shareholders.'},
}
