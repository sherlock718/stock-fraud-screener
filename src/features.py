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
    'shares_growth':         {'label': 'Share Count Growth',         'high_is': 'bad',     'fmt': '.4f',  'desc': 'YoY growth in shares outstanding. Rising share count signals ongoing dilution, often through equity issuance to fund losses.'},
    'shares_outstanding':    {'label': 'Shares Outstanding',         'high_is': 'neutral', 'fmt': '.0f',  'desc': 'Total diluted shares outstanding. Used as a size control variable in cross-sectional models.'},
    'log_revenue':           {'label': 'Revenue (log-scale)',        'high_is': 'neutral', 'fmt': '.2f',  'desc': 'Natural log of revenue. Used as a size control; large firms have structurally different fraud profiles.'},
    'entry_price':           {'label': 'Price at Entry',             'high_is': 'neutral', 'fmt': '.2f',  'desc': 'Stock price at the time of the filing snapshot. Used for calculating per-share metrics.'},
    # --- Efficiency & Cash Flow ---
    'asset_turnover':        {'label': 'Asset Turnover',             'high_is': 'good',    'fmt': '.3f',  'desc': 'Revenue / total assets. Measures how efficiently the company generates sales from its asset base.'},
    'cash_conversion':       {'label': 'Cash Conversion Cycle',      'high_is': 'neutral', 'fmt': '.1f',  'desc': 'Days inventory outstanding + days sales outstanding − days payable outstanding. Measures working capital efficiency.'},
    'ocf_to_assets':         {'label': 'OCF / Total Assets',         'high_is': 'good',    'fmt': '.3f',  'desc': 'Operating cash flow scaled by total assets. A quality metric separating cash-generative from accrual-heavy businesses.'},
    'ocf_to_debt':           {'label': 'OCF / Total Debt',           'high_is': 'good',    'fmt': '.3f',  'desc': 'Operating cash flow relative to total debt. Higher values indicate the company can service debt from operations.'},
    'gross_profit_to_assets':{'label': 'Gross Profit / Assets',      'high_is': 'good',    'fmt': '.3f',  'desc': 'Gross profit scaled by total assets. A robust profitability measure that is harder to manipulate than net income.'},
    'ocf_growth_yoy':        {'label': 'OCF Growth (YoY)',           'high_is': 'good',    'fmt': '.3f',  'desc': 'Year-on-year change in operating cash flow. Divergence from reported earnings growth is a red flag.'},
    'net_income_growth_yoy': {'label': 'Net Income Growth (YoY)',    'high_is': 'neutral', 'fmt': '.3f',  'desc': 'Year-on-year change in net income. Should be compared against OCF growth; large gaps flag earnings quality issues.'},
    'financing_cashflow_to_assets': {'label': 'Financing CFO / Assets', 'high_is': 'bad', 'fmt': '.3f',  'desc': 'Cash from financing activities scaled by total assets. Persistent reliance on financing for operations signals stress.'},
    # --- Balance Sheet Composition ---
    'goodwill_to_assets':    {'label': 'Goodwill / Total Assets',    'high_is': 'bad',     'fmt': '.3f',  'desc': 'Goodwill relative to total assets. High ratios indicate acquisition-heavy strategies where write-downs can mask past overvaluation.'},
    'soft_assets_ratio':     {'label': 'Soft Assets Ratio',          'high_is': 'bad',     'fmt': '.3f',  'desc': '(Total assets − PP&E − cash) / total assets. Measures the proportion of assets that are easy to fabricate (goodwill, intangibles, receivables).'},
    'total_liabilities':     {'label': 'Total Liabilities',          'high_is': 'neutral', 'fmt': '.0f',  'desc': 'Total reported liabilities. Used as a denominator in leverage ratios and as a size indicator.'},
    'other_noncurrent_assets':{'label': 'Other Non-Current Assets',  'high_is': 'bad',     'fmt': '.0f',  'desc': 'Non-current assets that are not PP&E, goodwill, or investments. A catch-all that can conceal capitalised costs.'},
    # --- Altman sub-components ---
    'altman_x3':             {'label': 'Altman X3 (EBIT/Assets)',    'high_is': 'good',    'fmt': '.3f',  'desc': 'EBIT / total assets — the operating returns component of the Altman Z-Score. Declining X3 with stable reported EPS is a warning.'},
    # --- Sector-relative percentiles ---
    'altman_z_score_sector_pct':  {'label': 'Z-Score (Sector Pct)', 'high_is': 'good',    'fmt': '.2f',  'desc': 'Altman Z-Score percentile within the same sector. A low percentile flags relative financial distress vs industry peers.'},
    'debt_to_assets_sector_pct':  {'label': 'Debt/Assets (Sector Pct)', 'high_is': 'bad', 'fmt': '.2f',  'desc': 'Debt-to-assets ratio percentile vs sector. Companies in the top decile for leverage within their industry are at higher risk.'},
    'ocf_to_ni_sector_pct':       {'label': 'OCF/NI (Sector Pct)',  'high_is': 'good',    'fmt': '.2f',  'desc': 'OCF-to-net-income percentile vs sector peers. Low cash conversion relative to peers is a relative manipulation signal.'},
    'ps_ratio_sector_pct':        {'label': 'P/S Ratio (Sector Pct)', 'high_is': 'neutral','fmt': '.2f',  'desc': 'Price-to-sales percentile vs sector. A very high P/S in a low-margin industry creates pressure to inflate revenue.'},
    # --- Piotroski sub-signals ---
    'piotroski_f_score_9':   {'label': 'Piotroski F-Score (9pt)',    'high_is': 'good',    'fmt': '.0f',  'desc': 'Full 9-point Piotroski score including profitability, leverage, and efficiency tests. Scores ≥ 6 indicate strong fundamentals.'},
    'piotroski_ocf_pos':     {'label': 'Piotroski: OCF Positive',    'high_is': 'good',    'fmt': '.0f',  'desc': 'Binary flag (1/0): operating cash flow is positive. A company with positive income but negative OCF fails this test.'},
    'piotroski_shares_ok':   {'label': 'Piotroski: No New Shares',   'high_is': 'good',    'fmt': '.0f',  'desc': 'Binary flag (1/0): share count did not increase. New share issuance under financial stress is a negative signal.'},
    'piotroski_delta_at':    {'label': 'Piotroski: Asset Turnover Δ','high_is': 'good',    'fmt': '.0f',  'desc': 'Binary flag (1/0): asset turnover improved YoY. Deteriorating asset turnover with stable margins suggests cost manipulation.'},
    'piotroski_delta_gm':    {'label': 'Piotroski: Gross Margin Δ',  'high_is': 'good',    'fmt': '.0f',  'desc': 'Binary flag (1/0): gross margin improved YoY. A declining gross margin is one of the earliest fraud pressure signals.'},
    # --- Proprietary sub-scores ---
    'fraud_score_composite': {'label': 'Fraud Score (Composite)',    'high_is': 'bad',     'fmt': '.3f',  'desc': 'Ensemble fraud probability combining accounting, dilution, distress, governance, and earnings quality sub-scores.'},
    'fraud_score_distress':  {'label': 'Fraud Score (Distress)',     'high_is': 'bad',     'fmt': '.3f',  'desc': 'Financial distress sub-score. High values indicate Z-Score, interest coverage, and debt sustainability all pointing to stress.'},
    # --- Valuation additions ---
    'ps_ratio':              {'label': 'P/S Ratio',                  'high_is': 'neutral', 'fmt': '.2f',  'desc': 'Price-to-sales ratio. Very high P/S creates incentive to sustain reported revenue growth at any cost.'},
    'ev_revenue':            {'label': 'EV / Revenue',               'high_is': 'neutral', 'fmt': '.2f',  'desc': 'Enterprise value relative to revenue. High EV/Revenue with weak margins signals growth expectations that may require manipulation to sustain.'},
    'value_composite':       {'label': 'Value Composite Score',      'high_is': 'good',    'fmt': '.3f',  'desc': 'Combined value factor score (P/E, P/B, P/S, EV/EBITDA, FCF yield). Higher values indicate cheaper valuations.'},
    # --- Ohlson component ---
    'ohlson_roe':            {'label': 'Ohlson: ROE Signal',         'high_is': 'good',    'fmt': '.3f',  'desc': 'Return on equity component within the Ohlson O-Score model. Negative ROE contributes to bankruptcy probability.'},
    # --- Market / macro ---
    'price_to_52w_high':     {'label': 'Price / 52-Week High',       'high_is': 'neutral', 'fmt': '.3f',  'desc': 'Current price relative to the 52-week high. Values far below 1.0 may indicate market has already priced in distress before disclosure.'},
    'vol_prior_12m':         {'label': 'Return Volatility (12M)',    'high_is': 'bad',     'fmt': '.4f',  'desc': 'Standard deviation of monthly returns over the prior 12 months. High volatility often precedes or accompanies fraud revelation.'},
    'vix':                   {'label': 'VIX (Market Fear Index)',    'high_is': 'neutral', 'fmt': '.1f',  'desc': 'CBOE Volatility Index at filing date. Used as a macro control for market-wide risk appetite.'},
    # --- Factor interaction signals ---
    'quality_composite':     {'label': 'Quality Composite Score',    'high_is': 'good',    'fmt': '.3f',  'desc': 'Combined quality factor (ROA, ROE, margins, accruals). High quality companies with anomalous accounting scores are especially suspicious.'},
    'quality_in_recession':  {'label': 'Quality × Recession Flag',  'high_is': 'neutral', 'fmt': '.3f',  'desc': 'Quality composite interacted with a recession indicator. Quality stocks underperform in normal times but fraud is more likely during downturns.'},
    'quality_x_momentum':    {'label': 'Quality × Momentum',        'high_is': 'neutral', 'fmt': '.3f',  'desc': 'Interaction of quality and price momentum factors. High quality with deteriorating momentum can signal early distress.'},
    'small_x_quality':       {'label': 'Size × Quality',            'high_is': 'neutral', 'fmt': '.3f',  'desc': 'Interaction of small-cap indicator with quality factor. Small firms with poor quality have historically higher fraud rates.'},
    'value_in_recession':    {'label': 'Value × Recession Flag',    'high_is': 'neutral', 'fmt': '.3f',  'desc': 'Value factor interacted with recession indicator. Deep value during recessions can indicate distress rather than opportunity.'},
    'value_x_momentum':      {'label': 'Value × Momentum',          'high_is': 'neutral', 'fmt': '.3f',  'desc': 'Interaction of value and momentum factors. Companies that are cheap and falling in price may be value traps or pre-fraud.'},
    'value_x_quality':       {'label': 'Value × Quality',           'high_is': 'neutral', 'fmt': '.3f',  'desc': 'Interaction of value and quality factors. The highest-conviction long candidates are both cheap and high-quality.'},
}
