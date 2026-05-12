"""
Step 5 — Compute all features from snapshots + prices + macro.

Zero API calls. Pure computation from parquet files produced by steps 2-4.
Fast re-run: ~5-10 minutes regardless of dataset size.

Feature groups (170+ total):
  A. Raw financial ratios (P/E, P/B, EV/EBITDA, etc.)
  B. Profitability metrics (ROA, ROE, ROIC, NOPAT, etc.)
  C. Accrual decomposition (RSST, working capital vs long-term, Sloan ratio)
  D. Fraud / quality signals (Beneish M-score sub-components, Altman Z, Ohlson O)
  E. Growth features (17 YoY + 6 3-year trends, from step 2)
  F. Momentum + volatility (from step 3)
  G. FRED macro context (from step 4)
  H. Interaction features (value × quality, quality × momentum, size effects)
  I. Sector-relative percentiles (within SIC 2-digit group)
  J. NOA growth and balance sheet bloat signals

Input:  data/snapshots.parquet, data/prices.parquet, data/macro.parquet
Output: data/historical_dataset.parquet
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

BASE = Path(__file__).parent.parent
DATA = BASE / 'data'

SNAP  = DATA / 'snapshots.parquet'
PRICE = DATA / 'prices.parquet'
MACRO = DATA / 'macro.parquet'
OUT   = DATA / 'historical_dataset.parquet'


# ── Safe arithmetic helpers ───────────────────────────────────────────────────

def sdiv(num, denom, fill=np.nan):
    """Safe divide: returns fill if denom is 0 or NaN."""
    num   = pd.to_numeric(num,   errors='coerce')
    denom = pd.to_numeric(denom, errors='coerce')
    with np.errstate(divide='ignore', invalid='ignore'):
        result = np.where(
            (denom.notna()) & (denom != 0),
            num / denom,
            fill
        )
    return pd.Series(result, index=num.index if hasattr(num, 'index') else None)


def slog(series):
    """Safe log — clips to small positive value."""
    s = pd.to_numeric(series, errors='coerce')
    return np.log(s.clip(lower=1e-10))


def winsorize(series: pd.Series, lower=0.01, upper=0.99) -> pd.Series:
    """Clip to percentile bounds to suppress outliers."""
    lo = series.quantile(lower)
    hi = series.quantile(upper)
    return series.clip(lo, hi)


# ── A. Valuation ratios ───────────────────────────────────────────────────────

def add_valuation(df: pd.DataFrame) -> pd.DataFrame:
    nan = pd.Series(np.nan, index=df.index)
    zero = pd.Series(0.0, index=df.index)
    mc  = df.get('market_cap_at_filing', nan)
    ep  = df.get('entry_price',          nan)
    rev = df.get('revenue',              nan)
    ni  = df.get('net_income',           nan)
    eq  = df.get('total_equity',         nan)
    ta  = df.get('total_assets',         nan)
    ocf = df.get('operating_cash_flow',  nan)
    lt  = df.get('long_term_debt',       zero).fillna(0)
    ca  = df.get('current_assets',       nan)
    cl  = df.get('current_liabilities',  nan)
    dep = df.get('depreciation', nan)
    da  = df.get('da_expense', dep)
    ebit = df.get('operating_income', nan)

    ebitda = ebit + da.fillna(0)
    nwc    = ca - cl
    ev     = mc + lt.fillna(0) - df.get('cash', pd.Series(0, index=df.index)).fillna(0)

    df['pe_ratio']          = sdiv(mc, ni)
    df['pb_ratio']          = sdiv(mc, eq)
    df['ps_ratio']          = sdiv(mc, rev)
    df['pcf_ratio']         = sdiv(mc, ocf)
    df['ev_ebitda']         = sdiv(ev, ebitda)
    df['ev_revenue']        = sdiv(ev, rev)
    df['ev_ocf']            = sdiv(ev, ocf)
    df['earnings_yield']    = sdiv(ni, mc)           # 1/PE — useful for ML
    df['book_to_market']    = sdiv(eq, mc)
    df['sales_to_price']    = sdiv(rev, mc)
    df['fcf_yield']         = sdiv(ocf - df.get('capex', pd.Series(0, index=df.index)).fillna(0), mc)
    df['net_debt_to_equity']= sdiv(lt.fillna(0) - df.get('cash', pd.Series(0, index=df.index)).fillna(0), eq)
    df['nwc_to_assets']     = sdiv(nwc, ta)

    return df


# ── B. Profitability ──────────────────────────────────────────────────────────

def add_profitability(df: pd.DataFrame) -> pd.DataFrame:
    nan  = pd.Series(np.nan, index=df.index)
    zero = pd.Series(0.0, index=df.index)
    ta   = df.get('total_assets',        nan)
    eq   = df.get('total_equity',        nan)
    rev  = df.get('revenue',             nan)
    gp   = df.get('gross_profit',        nan)
    oi   = df.get('operating_income',    nan)
    ni   = df.get('net_income',          nan)
    ocf  = df.get('operating_cash_flow', nan)
    lt   = df.get('long_term_debt',      zero).fillna(0)
    cash = df.get('cash', zero).fillna(0)

    invested_capital = eq + lt - cash

    df['roa']              = sdiv(ni, ta)
    df['roe']              = sdiv(ni, eq)
    df['roic']             = sdiv(oi, invested_capital)
    df['roa_operating']    = sdiv(oi, ta)
    df['gross_margin']     = sdiv(gp, rev)
    df['operating_margin'] = sdiv(oi, rev)
    df['net_margin']       = sdiv(ni, rev)
    df['asset_turnover']   = sdiv(rev, ta)
    df['ocf_margin']       = sdiv(ocf, rev)
    df['ocf_to_assets']    = sdiv(ocf, ta)
    df['ocf_to_ni']              = sdiv(ocf, ni)
    df['gross_profit_to_assets'] = sdiv(gp, ta)  # Novy-Marx (2013) — most robust profitability signal
    df['capex_intensity']        = sdiv(df.get('capex', pd.Series(np.nan, index=df.index)), rev)
    df['rd_intensity']           = sdiv(df.get('rd_expense', pd.Series(np.nan, index=df.index)), rev)
    df['sga_intensity']    = sdiv(df.get('sga_expense', pd.Series(np.nan, index=df.index)), rev)

    return df


# ── C. Accrual decomposition (RSST) ──────────────────────────────────────────

def add_accruals(df: pd.DataFrame) -> pd.DataFrame:
    """
    RSST accrual decomposition (Richardson et al. 2005):
      Total accruals = WC accruals + LT accruals + financial accruals
      WC accruals    = Δ(current assets ex-cash) - Δ(current liabilities ex-debt)
      LT accruals    = Δ(non-current operating assets) - Δ(non-current operating liabilities)
      Sloan ratio    = (net income - OCF) / avg total assets

    All are powerful negative predictors of forward returns (anomaly literature).
    """
    ta   = df.get('total_assets',        pd.Series(np.nan, index=df.index))
    ni   = df.get('net_income',          pd.Series(np.nan, index=df.index))
    ocf  = df.get('operating_cash_flow', pd.Series(np.nan, index=df.index))
    ca   = df.get('current_assets',      pd.Series(np.nan, index=df.index))
    cl   = df.get('current_liabilities', pd.Series(np.nan, index=df.index))
    cash = df.get('cash', pd.Series(0, index=df.index)).fillna(0)
    std  = df.get('short_term_debt', pd.Series(0, index=df.index)).fillna(0)
    rec  = df.get('accounts_receivable', pd.Series(np.nan, index=df.index))
    inv  = df.get('inventory', pd.Series(np.nan, index=df.index))
    ap   = df.get('accounts_payable', pd.Series(0, index=df.index)).fillna(0)
    rev  = df.get('revenue',             pd.Series(np.nan, index=df.index))

    # Sloan ratio (accruals / avg assets)
    df['sloan_accruals']  = sdiv(ni - ocf, ta)

    # Working capital accruals (Δ non-cash current assets - Δ non-debt current liabilities)
    # Uses YoY changes from step 2 if available
    delta_rec = df.get('receivables_growth', pd.Series(np.nan, index=df.index)) * rec
    delta_inv = df.get('inventory_growth',   pd.Series(np.nan, index=df.index)) * inv
    delta_ap  = df.get('ap_growth',          pd.Series(np.nan, index=df.index)) * ap

    wc_accruals = delta_rec.fillna(0) + delta_inv.fillna(0) - delta_ap.fillna(0)
    df['wc_accruals_to_assets'] = sdiv(wc_accruals, ta)
    # Richardson et al. (2005) separately named columns used in Phase B feature sets
    df['sloan_wc_accruals'] = sdiv(wc_accruals, ta)

    # Balance sheet accruals proxy: total accruals split
    df['accruals_to_assets'] = df['sloan_accruals']

    # LT accruals: change in non-current operating assets minus non-current operating liabilities
    # Approximation: total accruals - WC accruals
    lt_accruals = (ni - ocf) - wc_accruals.reindex(df.index).fillna(0)
    df['sloan_lt_accruals'] = sdiv(lt_accruals, ta)

    # NOA = Total assets - cash - financial assets - total liabilities + debt
    lt      = df['long_term_debt'].fillna(0)
    eq      = df['total_equity']
    fin_assets = cash  # simplification: cash + short term investments
    tl      = ta - eq
    noa     = ta - fin_assets - (tl - lt)
    df['noa'] = noa

    # NOA/assets (level)
    df['noa_to_assets'] = sdiv(noa, ta)

    # NOA growth uses YoY from step 2 if available
    if 'assets_growth' in df.columns:
        df['noa_growth'] = df['assets_growth']
    else:
        df['noa_growth'] = np.nan

    # Cash conversion quality
    df['cash_conversion'] = sdiv(ocf, ni)

    # Receivables minus revenue growth — channel stuffing signal (Bao et al. 2020)
    # Clip growth rates first to avoid outlier explosion (max observed: 1.2M)
    rec_growth = df.get('receivables_growth', pd.Series(np.nan, index=df.index)).clip(-0.99, 5)
    rev_growth = df.get('revenue_growth',     pd.Series(np.nan, index=df.index)).clip(-0.99, 5)
    df['receivables_minus_revenue_growth'] = rec_growth - rev_growth

    # Delta DSO — change in days sales outstanding
    # Computed from raw components (DSO column not yet available at this point)
    dso_curr = sdiv(rec.fillna(0) * 365, rev)
    dso_prev = dso_curr * sdiv(1 + rev_growth.fillna(0), 1 + rec_growth.fillna(0))
    df['delta_dso'] = (dso_curr - dso_prev).clip(-180, 180)  # cap at ±6 months change

    return df


# ── D. Fraud / distress scores ────────────────────────────────────────────────

def add_fraud_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Beneish M-score (8 ratios, 1999) — fraud manipulation detection
    Altman Z-score (1968) — bankruptcy distress
    Ohlson O-score (1980) — probability of financial failure

    We store sub-components as individual features (each has its own alpha)
    in addition to the composite scores.
    """
    nan  = pd.Series(np.nan, index=df.index)
    zero = pd.Series(0.0, index=df.index)
    rev  = df.get('revenue',              nan)
    ta   = df.get('total_assets',         nan)
    ni   = df.get('net_income',           nan)
    ocf  = df.get('operating_cash_flow',  nan)
    eq   = df.get('total_equity',         nan)
    cl   = df.get('current_liabilities',  nan)
    ca   = df.get('current_assets',       nan)
    lt   = df.get('long_term_debt', zero).fillna(0)
    rec  = df.get('accounts_receivable', nan)
    inv  = df.get('inventory',           nan)
    ppe  = df.get('ppe_net',             nan)
    dep  = df.get('depreciation',        nan)
    cogs = df.get('cogs',                nan)
    gp   = df.get('gross_profit',        nan)
    oi   = df.get('operating_income',    nan)
    sga  = df.get('sga_expense',         nan)
    mc   = df.get('market_cap_at_filing', nan)

    rev_growth = df.get('revenue_growth', pd.Series(np.nan, index=df.index))

    # ── Beneish M-score components ──
    # DSRI: Days Sales Receivable Index (rising = manipulation signal)
    # Clipped to [0.5, 3.0] — values outside this range are data errors not manipulation signals
    df['beneish_dsri'] = sdiv(sdiv(rec, rev), sdiv(rec * (1 + rev_growth.fillna(0)), rev)).clip(0.5, 3.0)

    # GMI: Gross Margin Index (falling margin = risk)
    prev_gm = sdiv(gp, rev) / (1 + df.get('gross_margin_change', pd.Series(0, index=df.index)).fillna(0))
    df['beneish_gmi'] = sdiv(prev_gm, sdiv(gp, rev))

    # AQI: Asset Quality Index — off-balance-sheet creep YoY
    # AQI = soft_assets_ratio_t / soft_assets_ratio_{t-1}
    # soft_assets = total assets excluding cash and PP&E (harder to verify)
    # AQI > 1 means more off-balance-sheet assets relative to prior year = manipulation signal
    ca_growth_  = df.get('current_assets_growth', pd.Series(0, index=df.index)).fillna(0).clip(-0.9, 10)
    ppe_growth_ = df.get('ppe_growth_yoy',        pd.Series(0, index=df.index)).fillna(0).clip(-0.9, 10)
    asset_growth_ = df.get('assets_growth',       pd.Series(0, index=df.index)).fillna(0).clip(-0.9, 10)
    ca_prev_  = sdiv(ca.fillna(0),  1 + ca_growth_)
    ppe_prev_ = sdiv(ppe.fillna(0), 1 + ppe_growth_)
    ta_prev_  = sdiv(ta,            1 + asset_growth_)
    soft_ratio_t    = 1 - sdiv(ca.fillna(0)    + ppe.fillna(0),    ta)
    soft_ratio_prev = 1 - sdiv(ca_prev_.fillna(0) + ppe_prev_.fillna(0), ta_prev_)
    df['soft_assets_ratio'] = soft_ratio_t  # level — standalone feature
    df['beneish_aqi']       = sdiv(soft_ratio_t, soft_ratio_prev).clip(0, 5)

    # SGI: Sales Growth Index
    df['beneish_sgi'] = 1 + rev_growth.fillna(0)

    # DEPI: Depreciation Index (prior dep rate / current dep rate; Beneish 1999)
    # DEPI > 1 means the firm slowed depreciation relative to prior year — earnings manipulation signal
    dep_growth_ = df.get('depreciation_growth', pd.Series(np.nan, index=df.index)).fillna(asset_growth_)
    dep_prev_   = sdiv(dep, 1 + dep_growth_.clip(-0.9, 10))
    dep_rate_t    = sdiv(dep,       dep       + ppe.fillna(0))
    dep_rate_prev = sdiv(dep_prev_, dep_prev_ + ppe_prev_.fillna(0))
    df['beneish_depi'] = sdiv(dep_rate_prev, dep_rate_t).clip(0, 5)

    # SGAI: SGA Expense Index
    df['beneish_sgai'] = sdiv(sdiv(sga, rev), sdiv(sga, rev) / (1 + rev_growth.fillna(0)).clip(0.5, 2))

    # LVGI: Leverage Index (increasing debt = risk)
    total_debt = lt + cl.fillna(0)
    df['beneish_lvgi'] = sdiv(total_debt, ta)

    # TATA: Total Accruals to Total Assets (Sloan — already computed above)
    df['beneish_tata'] = df.get('sloan_accruals', sdiv(ni - ocf, ta))

    # Composite M-score (Beneish 1999 coefficients)
    # M = -4.84 + 0.92*DSRI + 0.528*GMI + 0.404*AQI + 0.892*SGI
    #         + 0.115*DEPI - 0.172*SGAI + 4.679*TATA - 0.327*LVGI
    df['beneish_m_score'] = (
        -4.84
        + 0.92  * df['beneish_dsri'].fillna(1)
        + 0.528 * df['beneish_gmi'].fillna(1)
        + 0.404 * df['beneish_aqi'].fillna(1)
        + 0.892 * df['beneish_sgi'].fillna(1)
        + 0.115 * df['beneish_depi'].fillna(1)
        - 0.172 * df['beneish_sgai'].fillna(1)
        + 4.679 * df['beneish_tata'].fillna(0)
        - 0.327 * df['beneish_lvgi'].fillna(0)
    )
    # M > -1.78 → likely manipulator

    # ── Altman Z-score components (1968) ──
    wc   = ca - cl
    re   = eq - df.get('additional_paid_in_capital',
                        pd.Series(0, index=df.index)).fillna(0)  # approximation

    df['altman_x1'] = sdiv(wc,  ta)                              # working capital / assets
    df['altman_x2'] = sdiv(re,  ta)                              # retained earnings / assets
    df['altman_x3'] = sdiv(oi,  ta)                              # EBIT / assets
    total_liab      = (ta - eq).clip(lower=1e3)                  # avoid div-by-zero on near-zero liabilities
    # X4: use book equity when market cap is unavailable (Z''-Score variant — non-US markets)
    mc_or_book = mc.fillna(eq.clip(lower=0))
    df['altman_x4'] = sdiv(mc_or_book, total_liab).clip(upper=20)
    df['altman_x5'] = sdiv(rev, ta)                              # sales / assets

    df['altman_z_score'] = (
        1.2 * df['altman_x1'].fillna(0)
        + 1.4 * df['altman_x2'].fillna(0)
        + 3.3 * df['altman_x3'].fillna(0)
        + 0.6 * df['altman_x4'].fillna(0)
        + 1.0 * df['altman_x5'].fillna(0)
    ).clip(-50, 50)
    # Z < 1.81 → distress; > 2.99 → safe; clipped at ±50 to suppress outliers

    # ── Ohlson O-score (1980) — logit-based bankruptcy probability ──
    tl   = ta - eq
    nits = (ni < 0).astype(float)  # net income two-year negative flag (proxy: current year)

    df['ohlson_size']     = slog(ta)
    df['ohlson_leverage'] = sdiv(tl, ta)
    df['ohlson_wc']       = sdiv(wc, ta)
    df['ohlson_roe']      = sdiv(ni, ta)
    df['ohlson_nits']     = nits
    df['ohlson_ocf']      = sdiv(ocf, tl)

    # O = -1.32 - 0.407*SIZE + 6.03*TLTA - 1.43*WCTA + 0.076*CLCA
    #          - 1.72*OENEG - 2.37*NITA - 1.83*FUTL + 0.285*INTWO - 0.521*CHIN
    clca = sdiv(cl, ca)  # current liabilities / current assets
    oeneg = (eq < 0).astype(float)
    df['ohlson_o_score'] = (
        -1.32
        - 0.407 * df['ohlson_size'].fillna(0)
        + 6.03  * df['ohlson_leverage'].fillna(0)
        - 1.43  * df['ohlson_wc'].fillna(0)
        + 0.076 * clca.fillna(0)
        - 1.72  * oeneg
        - 2.37  * df['ohlson_roe'].fillna(0)
        - 1.83  * df['ohlson_ocf'].fillna(0)
        + 0.285 * nits
    )
    # Convert to probability: 1 / (1 + exp(-O))
    df['ohlson_prob_bankruptcy'] = 1 / (1 + np.exp(-df['ohlson_o_score'].clip(-20, 20)))

    return df


# ── D2. Montier C-Score ───────────────────────────────────────────────────────

def add_montier_c_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Montier C-Score (James Montier 2008) — forensic accounting red-flag index.
    Six binary variables (0/1 each), composite score = sum (range 0–6).
    Higher score = more manipulation risk signals present.

    Components:
      C1: Revenue from credit growing faster than revenue (receivables stuffing)
      C2: Falling depreciation rate (earnings management via reduced dep)
      C3: SG&A growing faster than revenue (cost shifting into overheads)
      C4: Rising receivables (days sales outstanding growth > 0)
      C5: Growing inventory relative to revenue (channel stuffing)
      C6: Falling cash conversion (OCF / net income declining)

    Reference: Montier, J. (2008). "Joining the dark side: Pirates, rats and short sellers."
    Dresdner Kleinwort Research.
    """
    nan  = pd.Series(np.nan, index=df.index)
    zero = pd.Series(0.0,    index=df.index)

    rev    = df.get('revenue',              nan)
    rec    = df.get('accounts_receivable',  nan)
    inv    = df.get('inventory',            nan)
    dep    = df.get('depreciation',         nan)
    ppe    = df.get('property_plant_equipment', nan)
    sga    = df.get('sga_expense',          nan)
    ni     = df.get('net_income',           nan)
    ocf    = df.get('operating_cash_flow',  nan)

    rec_growth = df.get('receivables_growth', nan)
    rev_growth = df.get('revenue_growth',     nan)
    inv_growth = df.get('inventory_growth',   nan)
    sga_growth = df.get('sga_growth_yoy',     nan)

    # C1: receivables growing faster than revenue (0/1)
    c1 = (rec_growth.fillna(0) > rev_growth.fillna(0)).astype(float)
    c1[rec_growth.isna() & rev_growth.isna()] = np.nan

    # C2: depreciation rate falling YoY — dep / (dep + ppe) this year < prior year
    dep_rate = sdiv(dep, dep.fillna(0) + ppe.fillna(0))
    dep_growth_ = df.get('depreciation_growth', zero).fillna(0)
    asset_growth_ = df.get('assets_growth', zero).fillna(0)
    dep_prev = sdiv(dep, 1 + dep_growth_.clip(-0.9, 10))
    ppe_prev = sdiv(ppe.fillna(0), 1 + asset_growth_.clip(-0.9, 10))
    dep_rate_prev = sdiv(dep_prev, dep_prev.fillna(0) + ppe_prev.fillna(0))
    c2 = (dep_rate < dep_rate_prev).astype(float)
    c2[(dep.isna()) | (ppe.isna())] = np.nan

    # C3: SG&A growing faster than revenue (overhead creep)
    c3 = (sga_growth.fillna(0) > rev_growth.fillna(0)).astype(float)
    c3[sga_growth.isna() & rev_growth.isna()] = np.nan

    # C4: receivables increasing (DSO rising = collection risk)
    c4 = (rec_growth.fillna(0) > 0).astype(float)
    c4[rec_growth.isna()] = np.nan

    # C5: inventory growing faster than revenue (unsold build-up)
    c5 = (inv_growth.fillna(0) > rev_growth.fillna(0)).astype(float)
    c5[inv_growth.isna() & rev_growth.isna()] = np.nan

    # C6: cash conversion (OCF/NI) declining — earnings outrunning cash
    cash_conv = sdiv(ocf, ni)
    prev_cash_conv = sdiv(ocf, ni) / (1 + df.get('net_income_growth', zero).fillna(0).clip(-0.9, 10))
    c6 = (cash_conv < prev_cash_conv).astype(float)
    c6[(ocf.isna()) | (ni.isna())] = np.nan

    df['montier_c1'] = c1
    df['montier_c2'] = c2
    df['montier_c3'] = c3
    df['montier_c4'] = c4
    df['montier_c5'] = c5
    df['montier_c6'] = c6

    # Composite: sum of available components scaled to [0,1]
    c_cols = ['montier_c1', 'montier_c2', 'montier_c3', 'montier_c4', 'montier_c5', 'montier_c6']
    available = df[c_cols].notna().sum(axis=1)
    df['montier_c_score'] = df[c_cols].sum(axis=1, min_count=3)  # require ≥3 components
    # Normalise to [0,1] so it's comparable to other scores
    df['montier_c_score'] = df['montier_c_score'] / available.clip(lower=1)

    return df


# ── E. Liquidity & solvency ───────────────────────────────────────────────────

def add_liquidity(df: pd.DataFrame) -> pd.DataFrame:
    nan  = pd.Series(np.nan, index=df.index)
    zero = pd.Series(0.0, index=df.index)
    ca  = df.get('current_assets',       nan)
    cl  = df.get('current_liabilities',  nan)
    inv = df.get('inventory',  zero).fillna(0)
    ta  = df.get('total_assets',         nan)
    eq  = df.get('total_equity',         nan)
    lt  = df.get('long_term_debt', zero).fillna(0)
    ni  = df.get('net_income',           nan)
    oi  = df.get('operating_income',     nan)
    rev = df.get('revenue',              nan)
    ocf = df.get('operating_cash_flow',  nan)
    cash= df.get('cash', zero).fillna(0)

    df['current_ratio']       = sdiv(ca, cl)
    df['quick_ratio']         = sdiv(ca - inv, cl)
    df['cash_ratio']          = sdiv(cash, cl)
    df['debt_to_equity']      = sdiv(lt, eq)
    df['debt_to_assets']      = sdiv(lt, ta)
    df['net_debt_to_ebitda']  = sdiv(lt - cash, oi.fillna(ni))
    df['interest_coverage']   = sdiv(oi, df.get('interest_expense', pd.Series(np.nan, index=df.index)))
    df['equity_ratio']        = sdiv(eq, ta)
    df['financial_leverage']  = sdiv(ta, eq)
    df['days_sales_outstanding'] = sdiv(df.get('accounts_receivable', pd.Series(np.nan, index=df.index)) * 365, rev)
    df['days_inventory']      = sdiv(inv * 365, df.get('cogs', rev))
    df['days_payable']        = sdiv(df.get('accounts_payable', pd.Series(np.nan, index=df.index)) * 365,
                                     df.get('cogs', rev))
    df['cash_conversion_cycle'] = (df['days_sales_outstanding'].fillna(0)
                                   + df['days_inventory'].fillna(0)
                                   - df['days_payable'].fillna(0))
    df['ocf_to_debt']         = sdiv(ocf, lt)
    df['piotroski_ocf_pos']   = (ocf > 0).astype(float)  # used in Piotroski F-score
    df['piotroski_roa_pos']   = (sdiv(ni, ta) > 0).astype(float)
    df['piotroski_delta_roa'] = df.get('net_income_growth', pd.Series(np.nan, index=df.index))
    df['piotroski_delta_lev'] = df.get('debt_growth', pd.Series(np.nan, index=df.index))
    # Piotroski F6: Δ(current_ratio) > 0 — original Piotroski 2000 criterion
    if 'ticker' in df.columns and 'fiscal_year' in df.columns:
        _cr_prev = (df.sort_values(['ticker', 'fiscal_year'])
                      .groupby('ticker')['current_ratio'].shift(1)
                      .reindex(df.index))
        df['piotroski_delta_liq'] = df['current_ratio'] - _cr_prev
    else:
        df['piotroski_delta_liq'] = df.get('current_assets_growth', pd.Series(np.nan, index=df.index))

    return df


# ── F. Composite scores ───────────────────────────────────────────────────────

def add_composite_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Piotroski F-score (0-9), simple quality score, simple value score."""

    # Piotroski F-score (9 binary signals)
    piotroski_components = [
        df.get('piotroski_roa_pos',   pd.Series(0, index=df.index)),
        df.get('piotroski_ocf_pos',   pd.Series(0, index=df.index)),
        (df.get('piotroski_delta_roa', pd.Series(np.nan, index=df.index)) > 0).astype(float),
        (df.get('ocf_to_ni', pd.Series(np.nan, index=df.index)) > 1).astype(float),
        (df.get('piotroski_delta_lev', pd.Series(np.nan, index=df.index)) < 0).astype(float),
        (df.get('piotroski_delta_liq', pd.Series(np.nan, index=df.index)) > 0).astype(float),
        (df.get('shares_growth', pd.Series(np.nan, index=df.index)) <= 0).astype(float),
        (df.get('gross_margin_change', pd.Series(np.nan, index=df.index)) > 0).astype(float),
        (df.get('asset_turnover_change', pd.Series(np.nan, index=df.index)) > 0).astype(float),
    ]
    df['piotroski_f_score'] = sum(
        c.fillna(0) for c in piotroski_components
    )

    # Quality composite (high = good, Z-score normalised internally)
    quality_signals = ['roa', 'roe', 'gross_margin', 'ocf_to_ni', 'ocf_margin']
    quality_vals = pd.DataFrame({s: df.get(s, pd.Series(np.nan, index=df.index))
                                  for s in quality_signals})
    df['quality_composite'] = quality_vals.rank(pct=True).mean(axis=1)

    # Value composite (high = cheap)
    value_signals = ['book_to_market', 'earnings_yield', 'sales_to_price', 'fcf_yield']
    value_vals = pd.DataFrame({s: df.get(s, pd.Series(np.nan, index=df.index))
                                 for s in value_signals})
    df['value_composite'] = value_vals.rank(pct=True).mean(axis=1)

    return df


# ── G. Size features ──────────────────────────────────────────────────────────

def add_size_features(df: pd.DataFrame) -> pd.DataFrame:
    mc = df['market_cap_at_filing']
    ta = df['total_assets']
    rev= df['revenue']

    df['log_market_cap'] = slog(mc)
    df['log_assets']     = slog(ta)
    df['log_revenue']    = slog(rev)

    # Size category (micro/small/mid/large)
    def size_cat(mc):
        if pd.isna(mc):  return np.nan
        if mc < 300e6:   return 0  # micro
        if mc < 2e9:     return 1  # small
        if mc < 10e9:    return 2  # mid
        return 3                   # large

    df['size_category'] = mc.apply(size_cat)
    df['size_category_label'] = df['size_category'].map(
        {0: 'micro', 1: 'small', 2: 'mid', 3: 'large'}
    )

    return df


# ── H. Cross-sectional momentum rank transforms ───────────────────────────────

def add_momentum_ranks(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cross-sectional percentile ranks within (fiscal_year, market).
    Separates momentum signal from level — a 20% return ranks differently in a
    bull year (low rank) vs a bear year (high rank). Jegadeesh & Titman 1993.
    """
    group_keys = [k for k in ['fiscal_year', 'market'] if k in df.columns]
    if not group_keys:
        return df

    def pct_rank(x: pd.Series) -> pd.Series:
        if x.notna().sum() < 10:  # cohort guard: sparse cohort ranks are noisy
            return pd.Series(np.nan, index=x.index)
        return x.rank(pct=True, na_option='keep')

    raw_cols = {
        'momentum_12m_rank': 'momentum_12m_prior',
        'momentum_6m_rank':  'momentum_6m_prior',
        'momentum_3m_rank':  'momentum_3m_prior',
    }
    for rank_col, raw_col in raw_cols.items():
        if raw_col in df.columns:
            df[rank_col] = df.groupby(group_keys)[raw_col].transform(pct_rank)

    # Volatility rank — inverted so low-vol = high rank (low-vol premium)
    if 'vol_prior_12m' in df.columns:
        df['vol_rank_12m'] = 1.0 - df.groupby(group_keys)['vol_prior_12m'].transform(pct_rank)

    # Composite momentum rank: mean of available horizon ranks
    rank_cols = [c for c in ['momentum_12m_rank', 'momentum_6m_rank', 'momentum_3m_rank']
                 if c in df.columns]
    if rank_cols:
        df['momentum_composite_rank'] = df[rank_cols].mean(axis=1)

    return df


# ── I. Interaction features ───────────────────────────────────────────────────

def add_interactions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Interaction features exploit that factor premiums vary in magnitude
    across size × quality × value dimensions.

    Key documented interactions:
      - Value × Quality (Asness et al. 2019: strongest documented)
      - Momentum × Quality (avoids value traps)
      - Small × Quality (size premium concentrated in quality names)
    """
    val = df.get('value_composite',   pd.Series(np.nan, index=df.index))
    qua = df.get('quality_composite', pd.Series(np.nan, index=df.index))
    mom = df.get('momentum_12m_prior', pd.Series(np.nan, index=df.index))
    siz = df.get('log_market_cap',    pd.Series(np.nan, index=df.index))
    acc = df.get('sloan_accruals',    pd.Series(np.nan, index=df.index))
    noa = df.get('noa_to_assets',     pd.Series(np.nan, index=df.index))
    roa = df.get('roa',               pd.Series(np.nan, index=df.index))
    bm  = df.get('book_to_market',    pd.Series(np.nan, index=df.index))

    mom_rank = df.get('momentum_12m_rank', mom.rank(pct=True))

    df['value_x_quality']    = val * qua
    df['value_x_momentum']   = val * mom_rank
    df['quality_x_momentum'] = qua * mom_rank
    df['small_x_quality']    = (1 / siz.clip(1)) * qua.fillna(0)

    # Anti-quality signals (negative interactions)
    df['value_x_accruals']   = bm * (-acc.fillna(0))  # cheap but low accruals = best value
    df['roa_x_noa_growth']   = roa * (-noa.fillna(0)) # profitable + low NOA growth = best quality

    # Momentum reversal interaction
    if 'momentum_3m_prior' in df.columns and 'momentum_12m_prior' in df.columns:
        df['momentum_consistency'] = (
            df['momentum_12m_prior'].fillna(0) - df['momentum_3m_prior'].fillna(0)
        )  # strong 12m but recent pullback = higher probability of continuation

    return df


# ── I. Sector-relative percentiles ───────────────────────────────────────────

def add_sector_percentiles(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each key metric, compute the within-sector percentile rank.
    Model learns that a P/E of 15x is cheap in utilities but rich in tech.

    Sector = SIC 2-digit code (e.g., SIC 73xx → sector 73).
    """
    if 'sic_code' not in df.columns:
        return df

    df['sic_2digit'] = pd.to_numeric(df['sic_code'], errors='coerce').floordiv(100).astype('Int64')

    rank_features = [
        'pe_ratio', 'pb_ratio', 'ps_ratio', 'ev_ebitda',
        'roa', 'roe', 'gross_margin', 'operating_margin',
        'debt_to_assets', 'current_ratio',
        'revenue_growth', 'net_income_growth', 'assets_growth',
        'sloan_accruals', 'beneish_m_score', 'altman_z_score',
        'momentum_12m_prior', 'ocf_to_ni',
    ]

    # Group by BOTH sector AND fiscal_year — without fiscal_year this ranks a 2005
    # company against 2005-2024 peers, which is lookahead across time.
    group_keys = ['sic_2digit', 'fiscal_year'] if 'fiscal_year' in df.columns else ['sic_2digit']
    for feat in rank_features:
        if feat not in df.columns:
            continue
        col_pct = f'{feat}_sector_pct'
        df[col_pct] = df.groupby(group_keys, observed=True)[feat].transform(
            lambda x: x.rank(pct=True, na_option='keep')
        )

    return df


# ── J. Macro interaction features ─────────────────────────────────────────────

def add_macro_interactions(df: pd.DataFrame) -> pd.DataFrame:
    """Macro × factor interactions: signals work differently across regimes."""
    ffr  = df.get('fed_funds_rate',    pd.Series(np.nan, index=df.index))
    rec  = df.get('recession',         pd.Series(0,      index=df.index))
    yc   = df.get('yield_curve',       pd.Series(np.nan, index=df.index))
    hy   = df.get('hy_spread',         pd.Series(np.nan, index=df.index))
    val  = df.get('value_composite',   pd.Series(np.nan, index=df.index))
    mom  = df.get('momentum_12m_prior',pd.Series(np.nan, index=df.index))
    qua  = df.get('quality_composite', pd.Series(np.nan, index=df.index))

    df['value_in_high_rate']    = val * (ffr > 3.0).astype(float)
    df['value_in_recession']    = val * rec.fillna(0)
    df['momentum_in_expansion'] = mom * (1 - rec.fillna(0))
    df['quality_in_recession']  = qua * rec.fillna(0)
    df['levered_in_tight_credit']= (
        df.get('debt_to_assets', pd.Series(np.nan, index=df.index)) *
        (hy > 5.0).astype(float)
    )  # highly levered companies underperform more when credit is tight

    return df


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    DATA.mkdir(exist_ok=True)
    print('Step 5 — Computing features (zero API calls)')

    for path, label in [(SNAP, 'snapshots'), (PRICE, 'prices'), (MACRO, 'macro')]:
        if not path.exists():
            print(f'ERROR: {path} not found — run prior steps first')
            sys.exit(1)

    print('  Loading snapshots ...')
    snap  = pd.read_parquet(SNAP)
    print(f'    {len(snap):,} rows')

    print('  Loading prices ...')
    price = pd.read_parquet(PRICE)
    print(f'    {len(price):,} rows')

    print('  Loading macro ...')
    macro = pd.read_parquet(MACRO)
    print(f'    {len(macro):,} rows')

    # ── Merge ──────────────────────────────────────────────────────────────────
    print('  Merging snapshots + prices ...')
    price_key = ['cik', 'ticker', 'filed_date', 'fiscal_year', 'fiscal_quarter', 'period_type']
    # Keep merge keys + columns that exist only in prices (not already in snap)
    price_keep = price_key + [c for c in price.columns
                               if c not in set(snap.columns) and c not in set(price_key)]

    df = snap.merge(
        price[price_keep],
        on=['cik', 'ticker', 'filed_date', 'fiscal_year', 'fiscal_quarter', 'period_type'],
        how='left'
    )
    print(f'    After price merge: {len(df):,} rows')

    print('  Merging with macro ...')
    macro_key  = ['cik', 'ticker', 'filed_date', 'fiscal_year', 'fiscal_quarter', 'period_type']
    # Keep merge keys + columns that exist only in macro (not already in df)
    macro_keep = macro_key + [c for c in macro.columns
                               if c not in set(df.columns) and c not in set(macro_key)]

    df = df.merge(
        macro[macro_keep],
        on=['cik', 'ticker', 'filed_date', 'fiscal_year', 'fiscal_quarter', 'period_type'],
        how='left'
    )
    print(f'    After macro merge: {len(df):,} rows')

    # ── Normalise column names from step2 → step5 conventions ────────────────
    # step2 uses descriptive suffix names; step5 uses shorter standard names.
    # We add alias columns without removing originals (safe to call multiple times).
    COLUMN_ALIASES = {
        'equity':                  'total_equity',
        'receivables':             'accounts_receivable',
        'revenue_growth_yoy':      'revenue_growth',
        'net_income_growth_yoy':   'net_income_growth',
        'asset_growth_yoy':        'assets_growth',
        'debt_growth_yoy':         'debt_growth',
        'receivables_growth_yoy':  'receivables_growth',
        'inventory_growth_yoy':    'inventory_growth',
        'ap_growth_yoy':           'ap_growth',
        'ocf_growth_yoy':          'ocf_growth',
        'capex_growth_yoy':        'capex_growth',
        'gross_profit_growth_yoy': 'gross_profit_growth',
        'sga_growth_yoy':          'sga_growth',
        'rd_growth_yoy':           'rd_growth',
        'eps_growth_yoy':          'eps_growth',
        'equity_change_yoy':       'equity_growth',
        'ppe_growth_yoy':          'ppe_growth',
        'cash_change_yoy':         'cash_growth',
        'cogs_growth_yoy':         'cogs_growth',
        'shares_dilution':         'shares_growth',
        'shares_outstanding':      'common_shares_outstanding',
    }
    # Coalesce columns that may exist in both src and dst (prefer src which has better coverage)
    COALESCE_ALIASES = {'equity', 'sga_expense'}
    for src, dst in COLUMN_ALIASES.items():
        if src not in df.columns:
            continue
        if dst not in df.columns:
            df[dst] = df[src]
        elif src in COALESCE_ALIASES:
            # src has better coverage than the sparse dst coming from snapshots
            df[dst] = df[src].combine_first(df[dst])
    # asset_growth_yoy maps to two names — handle the second explicitly
    if 'asset_growth_yoy' in df.columns and 'current_assets_growth' not in df.columns:
        df['current_assets_growth'] = df['asset_growth_yoy']

    # Ensure numeric dtypes on key financial columns
    numeric_cols = [
        'total_assets', 'total_equity', 'revenue', 'net_income',
        'operating_cash_flow', 'current_assets', 'current_liabilities',
        'long_term_debt', 'market_cap_at_filing', 'entry_price',
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Fill missing debt columns with 0
    for col in ['long_term_debt', 'short_term_debt', 'accounts_payable',
                'interest_expense', 'cash']:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    # ── Feature computation ────────────────────────────────────────────────────
    print('  Computing valuation ratios ...')
    df = add_valuation(df)

    print('  Computing profitability metrics ...')
    df = add_profitability(df)

    print('  Computing accrual decomposition ...')
    df = add_accruals(df)

    print('  Computing fraud / distress scores ...')
    df = add_fraud_scores(df)

    print('  Computing Montier C-Score ...')
    df = add_montier_c_score(df)

    print('  Computing liquidity & solvency ...')
    df = add_liquidity(df)

    print('  Computing composite scores ...')
    df = add_composite_scores(df)

    print('  Computing size features ...')
    df = add_size_features(df)

    print('  Computing cross-sectional momentum ranks ...')
    df = add_momentum_ranks(df)

    print('  Computing interaction features ...')
    df = add_interactions(df)

    print('  Computing sector-relative percentiles ...')
    df = add_sector_percentiles(df)

    print('  Computing macro interaction features ...')
    df = add_macro_interactions(df)

    # Earnings stability — 5yr rolling std of ROE per company (lower = higher quality)
    # Requires time-series groupby; computed here after all other features exist
    print('  Computing earnings stability (5yr ROE volatility) ...')
    df = df.sort_values(['ticker', 'fiscal_year', 'fiscal_quarter'])
    df['roe_volatility_5yr'] = (
        df.groupby('ticker')['roe']
        .transform(lambda x: x.rolling(5, min_periods=3).std())
    )
    df['earnings_stability_5yr'] = -df['roe_volatility_5yr']  # invert: more stable = higher score

    if 'roa' in df.columns:
        df['roa_volatility_5yr'] = (
            df.groupby('ticker')['roa']
            .transform(lambda x: x.rolling(5, min_periods=3).std())
        )
        df['earnings_stability_roa_5yr'] = -df['roa_volatility_5yr']

    # ── Winsorize key ratios and growth features ──────────────────────────────
    # Rule: ALL growth _yoy columns must be winsorized here (pipeline-integrity Rule 6).
    # Without winsorization, companies growing from near-zero (revenue 1 → 184,343×) or
    # extreme micro-cap EPS swings dominate LightGBM splits and inflate IC estimates.
    print('  Winsorizing extreme values (1st-99th percentile) ...')
    ratio_cols = [
        # Valuation ratios
        'pe_ratio', 'pb_ratio', 'ps_ratio', 'ev_ebitda', 'ev_revenue',
        # Profitability
        'roa', 'roe', 'roic', 'sloan_accruals', 'beneish_m_score',
        'altman_z_score', 'ohlson_o_score', 'ocf_to_ni',
        'days_sales_outstanding', 'days_inventory', 'cash_conversion_cycle',
        'receivables_minus_revenue_growth', 'delta_dso',
        'gross_profit_to_assets', 'earnings_stability_5yr',
        # Growth YoY — must be winsorized: near-zero base causes extreme multiples
        'revenue_growth_yoy', 'revenue_growth',
        'net_income_growth_yoy', 'net_income_growth',
        'asset_growth_yoy', 'assets_growth',
        'eps_growth_yoy', 'eps_growth',
        'gross_profit_growth_yoy',
        'ocf_growth_yoy', 'ocf_growth',
        'capex_growth_yoy', 'capex_growth',
        'receivables_growth_yoy', 'receivables_growth',
        'inventory_growth_yoy', 'inventory_growth',
        'ap_growth_yoy', 'ap_growth',
        'debt_growth_yoy', 'debt_growth',
        'lt_debt_growth_yoy',
        'cogs_growth_yoy', 'cogs_growth',
        'sga_growth_yoy', 'sga_growth',
        'rd_growth_yoy', 'rd_growth',
        'ppe_growth_yoy', 'ppe_growth',
        'equity_growth', 'equity_change_yoy',
        'shares_dilution', 'shares_growth',
        # Forensic accounting composites
        'montier_c_score', 'wc_accruals_to_assets', 'sloan_wc_accruals', 'sloan_lt_accruals',
    ]
    for col in ratio_cols:
        if col in df.columns:
            df[col] = winsorize(df[col].astype(float))

    # ── Save ───────────────────────────────────────────────────────────────────
    print(f'  Saving {len(df):,} rows × {len(df.columns)} columns ...')
    df.to_parquet(OUT, index=False)

    # Feature summary
    feat_groups = {
        'Raw financials (step 2)': [c for c in df.columns if c in snap.columns and c not in
                                    ['cik','ticker','name','filed_date','fiscal_year',
                                     'fiscal_quarter','period_type','exchange','sic_code',
                                     'sic_description','market','country','accounting_std']],
        'Valuation ratios':        [c for c in df.columns if any(c.startswith(p) for p in
                                    ['pe_','pb_','ps_','pcf_','ev_','earnings_yield','book_to',
                                     'sales_to','fcf_','net_debt'])],
        'Profitability':           [c for c in df.columns if any(c.startswith(p) for p in
                                    ['roa','roe','roic','gross_margin','operating_margin',
                                     'net_margin','asset_turn','ocf_','capex_','rd_','sga_'])],
        'Accruals / NOA':          [c for c in df.columns if any(p in c for p in
                                    ['accrual','sloan','noa','cash_conv'])],
        'Fraud / distress scores': [c for c in df.columns if any(p in c for p in
                                    ['beneish','altman','ohlson','piotroski'])],
        'Forward returns':         [c for c in df.columns if 'forward_return' in c
                                    or 'beat_local' in c or 'excess_return' in c],
        'Macro':                   [c for c in df.columns if any(c.startswith(p) for p in
                                    ['treasury','yield_curve','fed_funds','credit_spread',
                                     'hy_spread','cpi','recession','vix','real_rate',
                                     'credit_tighten','macro_regime'])],
        'Interactions':            [c for c in df.columns if '_x_' in c or 'composite' in c
                                    or c.endswith('_in_high_rate') or c.endswith('_in_recession')],
        'Sector percentiles':      [c for c in df.columns if c.endswith('_sector_pct')],
        'Momentum ranks':          [c for c in df.columns if c.endswith('_rank') and 'momentum' in c
                                    or c == 'vol_rank_12m'],
    }

    print(f'\nStep 5 complete.')
    print(f'  Total features: {len(df.columns)}')
    for group, cols in feat_groups.items():
        print(f'  {group}: {len(cols)} features')
    print(f'  Rows: {len(df):,} | Tickers: {df["ticker"].nunique():,}')
    print(f'  Saved: {OUT}')


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Step 5 — Compute features')
    parser.add_argument('--snapshots', type=str, default=None, help='Path to snapshots parquet')
    parser.add_argument('--prices',    type=str, default=None, help='Path to prices parquet')
    parser.add_argument('--macro',     type=str, default=None, help='Path to macro parquet')
    parser.add_argument('--suffix',    type=str, default='',   help='Market suffix, e.g. _br')
    args = parser.parse_args()

    sfx = args.suffix
    if args.snapshots:
        SNAP = Path(args.snapshots)
    if sfx:
        OUT   = DATA / f'historical_dataset{sfx}.parquet'
        PRICE = DATA / f'prices{sfx}.parquet'
        MACRO = DATA / f'macro{sfx}.parquet'
    if args.prices:
        PRICE = Path(args.prices)
    if args.macro:
        MACRO = Path(args.macro)
    run()
