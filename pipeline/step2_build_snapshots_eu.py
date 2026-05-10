"""
Step 2 EU — Build financial snapshots from yfinance (European markets, free).

Reads tickers_eu.parquet built by step1_fetch_tickers_eu.py and fetches
4-5 years of annual fundamentals via yfinance.  No API key required.

Markets covered: DE, FR, NL, BE, PT, NO, FI, DK, SE, IE (and any others
in tickers_eu.parquet).

For deeper historical data (10+ years) SimFin Premium (~$9–30/mo depending on
plan) is the best upgrade path.  Set SIMFIN_API_KEY in .env and swap the data
source here.

Output: data/snapshots_eu.parquet — same schema as snapshots.parquet,
  market=<country code>, accounting_std='IFRS'
"""

from __future__ import annotations

import time
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf

BASE  = Path(__file__).parent.parent
DATA  = BASE / 'data'
OUT   = DATA / 'snapshots_eu.parquet'
TICK  = DATA / 'tickers_eu.parquet'

RATE_DELAY = 0.3   # yfinance rate limit


# ── yfinance field mappings (same as CA pipeline) ─────────────────────────────

INCOME_FIELDS = {
    'Total Revenue':                        'revenue',
    'Cost Of Revenue':                      'cogs',
    'Gross Profit':                         'gross_profit',
    'Operating Income':                     'operating_income',
    'Pretax Income':                        'pretax_income',
    'Tax Provision':                        'tax_expense',
    'Net Income':                           'net_income',
    'Basic EPS':                            'eps_basic',
    'Diluted EPS':                          'eps_diluted',
    'Basic Average Shares':                 'shares_outstanding',
    'Research And Development':             'rd_expense',
    'Selling General And Administration':   'sga_expense',
    'Interest Expense':                     'interest_expense',
    'EBITDA':                               'ebitda',
    'Reconciled Depreciation':              'depreciation_amortization',
    'Total Unusual Items':                  'non_operating_income',
    'Normalized EBITDA':                    'ebitda',
    'Diluted Average Shares':               'shares_outstanding',
}

BALANCE_FIELDS = {
    'Total Assets':                              'total_assets',
    'Total Liabilities Net Minority Interest':   'total_liabilities',
    'Stockholders Equity':                       'equity',
    'Current Assets':                            'current_assets',
    'Current Liabilities':                       'current_liabilities',
    'Cash And Cash Equivalents':                 'cash',
    'Accounts Receivable':                       'receivables',
    'Inventory':                                 'inventory',
    'Net PPE':                                   'ppe_net',
    'Long Term Debt':                            'long_term_debt',
    'Short Term Debt':                           'short_term_debt',
    'Retained Earnings':                         'retained_earnings',
    'Goodwill':                                  'goodwill',
    'Intangible Assets':                         'intangibles',
    'Payables':                                  'accounts_payable',
}

CASHFLOW_FIELDS = {
    'Operating Cash Flow':               'operating_cash_flow',
    'Capital Expenditure':               'capex',
    'Free Cash Flow':                    'fcf',
    'Investing Cash Flow':               'cfi',
    'Financing Cash Flow':               'financing_cash_flow',
    'Dividends Paid':                    'dividends_paid',
    'Depreciation And Amortization':     'depreciation_amortization',
    'Change In Receivables':             'ar_to_revenue_change',
}


def fetch_yfinance_fundamentals(ticker_str: str) -> list[dict]:
    rows = []
    try:
        tkr = yf.Ticker(ticker_str)
        inc = tkr.financials
        bal = tkr.balance_sheet
        cf  = tkr.cashflow

        if inc is None or inc.empty:
            return []

        for col in inc.columns:
            date_str = str(col)[:10]
            row: dict = {
                'filed_date':     date_str,
                'fiscal_year':    date_str[:4],
                'fiscal_quarter': 'FY',
                'period_type':    'annual',
            }

            for yf_field, our_field in INCOME_FIELDS.items():
                if yf_field in inc.index and col in inc.columns:
                    val = inc.loc[yf_field, col]
                    if pd.notna(val):
                        row[our_field] = float(val)

            if bal is not None and col in bal.columns:
                for yf_field, our_field in BALANCE_FIELDS.items():
                    if yf_field in bal.index:
                        val = bal.loc[yf_field, col]
                        if pd.notna(val):
                            row[our_field] = float(val)

            if cf is not None and col in cf.columns:
                for yf_field, our_field in CASHFLOW_FIELDS.items():
                    if yf_field in cf.index:
                        val = cf.loc[yf_field, col]
                        if pd.notna(val):
                            row[our_field] = float(val)

            if 'capex' in row:
                row['capex'] = abs(row['capex'])

            rows.append(row)

    except Exception:
        pass

    return rows


# ── YoY growth features ────────────────────────────────────────────────────────

def compute_yoy(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(['cik', 'filed_date']).copy()
    grow_pairs = [
        ('revenue',             'revenue_growth_yoy'),
        ('total_assets',        'asset_growth_yoy'),
        ('total_liabilities',   'debt_growth_yoy'),
        ('receivables',         'receivables_growth_yoy'),
        ('inventory',           'inventory_growth_yoy'),
        ('gross_profit',        'gross_profit_growth_yoy'),
        ('operating_cash_flow', 'ocf_growth_yoy'),
        ('net_income',          'net_income_growth_yoy'),
        ('equity',              'equity_change_yoy'),
        ('sga_expense',         'sga_growth_yoy'),
        ('capex',               'capex_growth_yoy'),
        ('ppe_net',             'ppe_growth_yoy'),
        ('eps_diluted',         'eps_growth_yoy'),
        ('long_term_debt',      'lt_debt_growth_yoy'),
    ]
    for _, rows in df.groupby('cik'):
        idx = rows.index
        for src, dst in grow_pairs:
            if src in df.columns:
                prev  = df.loc[idx, src].shift(1)
                curr  = df.loc[idx, src]
                with np.errstate(divide='ignore', invalid='ignore'):
                    growth = np.where(
                        prev.notna() & (prev != 0) & curr.notna(),
                        (curr - prev) / prev.abs(), np.nan,
                    )
                df.loc[idx, dst] = growth
    return df


# ── Currency lookup (for informational metadata) ──────────────────────────────

COUNTRY_CURRENCY = {
    'FR': 'EUR', 'NL': 'EUR', 'BE': 'EUR', 'PT': 'EUR',
    'FI': 'EUR', 'IE': 'EUR', 'DE': 'EUR', 'LU': 'EUR',
    'NO': 'NOK', 'DK': 'DKK', 'SE': 'SEK', 'GB': 'GBP',
    'IT': 'EUR', 'ES': 'EUR', 'AT': 'EUR', 'GR': 'EUR',
}


# ── Main ───────────────────────────────────────────────────────────────────────

def run(limit: int | None = None, countries: list[str] | None = None):
    DATA.mkdir(exist_ok=True)

    if not TICK.exists():
        raise FileNotFoundError(
            f'{TICK} not found — run step1_fetch_tickers_eu.py first'
        )

    tickers = pd.read_parquet(TICK)
    if countries:
        tickers = tickers[tickers['country'].isin(countries)].copy()
    if limit:
        tickers = tickers.head(limit).copy()
        print(f'TEST MODE: limited to {len(tickers):,} tickers')

    print(f'Step 2 EU — Building snapshots via yfinance for {len(tickers):,} companies')
    print(f'  NOTE: yfinance provides ~4-5 years of annual data per company')
    print(f'  Countries: {sorted(tickers["country"].unique().tolist())}')

    all_rows = []
    done = 0
    skipped = 0

    for _, tkr_row in tickers.iterrows():
        ticker_str = tkr_row.get('ticker', '')
        cik        = tkr_row.get('cik', ticker_str)
        name       = tkr_row.get('name', '')
        country    = tkr_row.get('country', '')
        exchange   = tkr_row.get('exchange', '')
        currency   = tkr_row.get('currency',
                                  COUNTRY_CURRENCY.get(str(country).upper(), 'EUR'))

        if not ticker_str:
            skipped += 1
            continue

        time.sleep(RATE_DELAY)
        rows = fetch_yfinance_fundamentals(ticker_str)

        if not rows:
            skipped += 1
        else:
            for row in rows:
                row.update({
                    'cik':            str(cik),
                    'ticker':         ticker_str,
                    'stock_code':     tkr_row.get('stock_code', ticker_str),
                    'name':           name,
                    'exchange':       exchange,
                    'market':         str(country).upper(),
                    'country':        str(country).upper(),
                    'currency':       currency,
                    'accounting_std': 'IFRS',
                })
            all_rows.extend(rows)
            done += 1

        if (done + skipped) % 100 == 0:
            print(f'  [{done + skipped:,}/{len(tickers):,}] '
                  f'{done:,} with data, {skipped:,} skipped, '
                  f'{len(all_rows):,} rows')

    if not all_rows:
        print('ERROR: no data loaded — check tickers_eu.parquet and yfinance access')
        import sys; sys.exit(1)

    combined = pd.DataFrame(all_rows)

    # total_debt convenience column
    lt = combined.get('long_term_debt',  pd.Series(0, index=combined.index)).fillna(0)
    st = combined.get('short_term_debt', pd.Series(0, index=combined.index)).fillna(0)
    combined['total_debt'] = (lt + st).replace(0, np.nan)

    print('\n  Computing YoY growth features ...')
    combined = compute_yoy(combined)

    combined.to_parquet(OUT, index=False)

    print(f'\nStep 2 EU complete.')
    print(f'  Total rows:       {len(combined):,}')
    print(f'  Unique companies: {combined["cik"].nunique():,}')
    print(f'  Annual rows:      {(combined["period_type"]=="annual").sum():,}')
    print(f'  Markets:          {combined["market"].value_counts().to_dict()}')
    print(f'  Date range:       {combined["filed_date"].min()} → {combined["filed_date"].max()}')
    print(f'  Limitation:       yfinance ~4-5 years. Upgrade: SimFin Premium for 10+ years.')
    print(f'  Saved: {OUT}')


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=None,
                        help='Limit companies (for testing)')
    parser.add_argument('--countries', nargs='+', default=None,
                        help='Filter to specific countries e.g. --countries FR DE')
    args = parser.parse_args()
    run(limit=args.limit, countries=args.countries)
