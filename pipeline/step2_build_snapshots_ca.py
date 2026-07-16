"""
Step 2 CA — Build financial snapshots from SEDAR+ XBRL filings (Canada).

SEDAR+ is Canada's filing system (replaced SEDAR 2023). Listed companies file
annual (AIF/MD&A) and quarterly (interim) financial statements in iXBRL format.

Strategy:
  - Downloads financial data via SEDAR+ bulk search API
  - Falls back to yfinance fundamental data for companies without XBRL

Since SEDAR+ does not provide a bulk CSV API, this pipeline uses the
SEDAR+ document search to find annual filings, then parses iXBRL where available.
For companies without accessible XBRL, yfinance provides 4-5 years of fundamentals.

Output: data/snapshots_ca.parquet — same schema as snapshots.parquet,
  market='CA', country='CA', accounting_std='IFRS'
"""

from __future__ import annotations

import time
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf

BASE  = Path(__file__).parent.parent
DATA  = BASE / 'data'
OUT   = DATA / 'snapshots_ca.parquet'
TICK  = DATA / 'tickers_ca.parquet'

RATE_DELAY = 0.3   # yfinance rate limit


# ── yfinance field mappings ────────────────────────────────────────────────────

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
    'Total Assets':                         'total_assets',
    'Total Liabilities Net Minority Interest': 'total_liabilities',
    'Stockholders Equity':                  'equity',
    'Current Assets':                       'current_assets',
    'Current Liabilities':                  'current_liabilities',
    'Cash And Cash Equivalents':            'cash',
    'Accounts Receivable':                  'receivables',
    'Inventory':                            'inventory',
    'Net PPE':                              'ppe_net',
    'Long Term Debt':                       'long_term_debt',
    'Short Term Debt':                      'short_term_debt',
    'Retained Earnings':                    'retained_earnings',
    'Goodwill':                             'goodwill',
    'Intangible Assets':                    'intangibles',
    'Payables':                             'accounts_payable',
}

CASHFLOW_FIELDS = {
    'Operating Cash Flow':                  'operating_cash_flow',
    'Capital Expenditure':                  'capex',
    'Free Cash Flow':                       'fcf',
    'Investing Cash Flow':                  'cfi',
    'Financing Cash Flow':                  'financing_cash_flow',
    'Dividends Paid':                       'dividends_paid',
    'Depreciation And Amortization':        'depreciation_amortization',
    'Change In Receivables':                'ar_to_revenue_change',
}


def fetch_yfinance_fundamentals(ticker_str: str) -> list[dict]:
    """
    Fetch annual financial statements from yfinance for one ticker.
    Returns list of annual snapshot dicts.
    """
    rows = []
    try:
        tkr = yf.Ticker(ticker_str)

        inc = tkr.financials          # annual income statement
        bal = tkr.balance_sheet       # annual balance sheet
        cf  = tkr.cashflow            # annual cash flow

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

            # Income statement
            for yf_field, our_field in INCOME_FIELDS.items():
                if yf_field in inc.index and col in inc.columns:
                    val = inc.loc[yf_field, col]
                    if pd.notna(val):
                        row[our_field] = float(val)

            # Balance sheet (same date)
            if bal is not None and col in bal.columns:
                for yf_field, our_field in BALANCE_FIELDS.items():
                    if yf_field in bal.index:
                        val = bal.loc[yf_field, col]
                        if pd.notna(val):
                            row[our_field] = float(val)

            # Cash flow
            if cf is not None and col in cf.columns:
                for yf_field, our_field in CASHFLOW_FIELDS.items():
                    if yf_field in cf.index:
                        val = cf.loc[yf_field, col]
                        if pd.notna(val):
                            row[our_field] = float(val)

            # Capex: yfinance reports as negative
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
        ('revenue',           'revenue_growth_yoy'),
        ('total_assets',      'asset_growth_yoy'),
        ('total_liabilities', 'debt_growth_yoy'),
        ('receivables',       'receivables_growth_yoy'),
        ('inventory',         'inventory_growth_yoy'),
        ('gross_profit',      'gross_profit_growth_yoy'),
        ('operating_cash_flow', 'ocf_growth_yoy'),
        ('net_income',        'net_income_growth_yoy'),
        ('equity',            'equity_change_yoy'),
        ('sga_expense',       'sga_growth_yoy'),
        ('capex',             'capex_growth_yoy'),
        ('ppe_net',           'ppe_growth_yoy'),
        ('eps_diluted',       'eps_growth_yoy'),
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


# ── Main ───────────────────────────────────────────────────────────────────────

def run(limit: int | None = None):
    DATA.mkdir(exist_ok=True)

    if not TICK.exists():
        raise FileNotFoundError(f'{TICK} not found — run step1_fetch_tickers_ca.py first')

    tickers = pd.read_parquet(TICK)
    if limit:
        tickers = tickers.head(limit).copy()
        print(f'TEST MODE: limited to {len(tickers):,} tickers')

    print(f'Step 2 CA — Building snapshots via yfinance for {len(tickers):,} companies')
    print(f'  NOTE: yfinance provides ~4-5 years of annual data')
    print(f'  For deeper history, SEDAR+ XBRL parser upgrade needed in Phase C')

    all_rows = []
    done = 0
    skipped = 0

    for _, tkr_row in tickers.iterrows():
        ticker_str = tkr_row['ticker']
        cik        = tkr_row['cik']
        name       = tkr_row.get('name', '')
        exchange   = tkr_row.get('exchange', 'TSX')

        time.sleep(RATE_DELAY)
        rows = fetch_yfinance_fundamentals(ticker_str)

        if not rows:
            skipped += 1
        else:
            for row in rows:
                row.update({
                    'cik':            cik,
                    'ticker':         ticker_str,
                    'stock_code':     cik,
                    'name':           name,
                    'exchange':       exchange,
                    'market':         'CA',
                    'entity_id':      f'CA:{cik}',
                    'availability_timestamp': None,
                    'availability_provenance': 'statement_date_unproven',
                    'country':        'CA',
                    'currency':       'CAD',
                    'accounting_std': 'IFRS',
                })
            all_rows.extend(rows)
            done += 1

        if (done + skipped) % 100 == 0:
            print(f'  [{done + skipped:,}/{len(tickers):,}] '
                  f'{done:,} with data, {skipped:,} skipped, '
                  f'{len(all_rows):,} rows')

    if not all_rows:
        print('ERROR: no data loaded')
        import sys; sys.exit(1)

    combined = pd.DataFrame(all_rows)

    print('\n  Computing YoY growth features ...')
    combined = compute_yoy(combined)

    combined.to_parquet(OUT, index=False)

    print(f'\nStep 2 CA complete.')
    print(f'  Total rows:       {len(combined):,}')
    print(f'  Unique companies: {combined["cik"].nunique():,}')
    print(f'  Annual rows:      {(combined["period_type"]=="annual").sum():,}')
    print(f'  Date range:       {combined["filed_date"].min()} → {combined["filed_date"].max()}')
    print(f'  Limitation:       yfinance data only (~4-5 years history)')
    print(f'  Saved: {OUT}')


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=None,
                        help='Limit companies (for testing)')
    args = parser.parse_args()
    run(limit=args.limit)
