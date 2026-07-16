"""
Step 2 BR — Build financial snapshots from CVM bulk CSV downloads (Brazil).

Downloads annual DFP (Demonstrações Financeiras Padronizadas, 2010-2025) and
quarterly ITR (Informações Trimestrais, 2011-2025) zip files from CVM.
All companies file IFRS since 2010.

CVM account code mapping (non-financial companies):
  DRE (income):  3.01=revenue, 3.02=cogs, 3.03=gross_profit, 3.05=op_income, 3.11=net_income
  BPA (assets):  1=total_assets, 1.01=current_assets, 1.01.01=cash, 1.01.03=receivables, 1.01.04=inventory
  BPP (liab+eq): 2.01=current_liab, 2.02=long_term_liab, 2.03=equity
  DFC (cf):      6.01=operating_cf, 6.02=investing_cf, 6.03=financing_cf

Output: data/snapshots_br.parquet — same schema as snapshots.parquet
"""

from __future__ import annotations

import io
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import requests

BASE  = Path(__file__).parent.parent
DATA  = BASE / 'data'
OUT   = DATA / 'snapshots_br.parquet'
TICK  = DATA / 'tickers_br.parquet'

CVM_DFP_BASE = 'https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS'
CVM_ITR_BASE = 'https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS'

START_YEAR = 2010

# ── Account code → schema column ──────────────────────────────────────────────
# Uses CD_CONTA (account code) as primary key.
# For financial companies some codes differ — we use DS_CONTA keyword validation.

# Income statement (DRE)
DRE_MAP: dict[str, tuple[str, str]] = {
    # code: (our_column, keyword that must appear in DS_CONTA — '' = accept any)
    '3.01': ('revenue',          'receita'),
    '3.02': ('cogs',             'custo'),
    '3.03': ('gross_profit',     'bruto'),
    '3.05': ('operating_income', 'financeiro'),   # "Resultado Antes do Resultado Financeiro"
    '3.07': ('pretax_income',    'tributo'),
    '3.08': ('tax_expense',      'imposto'),
    '3.11': ('net_income',       'l'),             # Lucro/Prejuízo Consolidado
    '3.99': ('eps_diluted',      ''),
}

# Balance sheet assets (BPA)
BPA_MAP: dict[str, tuple[str, str]] = {
    '1':       ('total_assets',    ''),
    '1.01':    ('current_assets',  'circulante'),
    '1.01.01': ('cash',            'caixa'),
    '1.01.03': ('receivables',     'receber'),
    '1.01.04': ('inventory',       'estoque'),
}

# Balance sheet liabilities + equity (BPP)
BPP_MAP: dict[str, tuple[str, str]] = {
    '2.01': ('current_liabilities', 'circulante'),
    '2.02': ('long_term_debt',      'n'),           # "Passivo Não Circulante" — proxy for LT liab
    '2.03': ('equity',              'patrim'),
}

# Cash flow (DFC indirect method preferred)
DFC_MAP: dict[str, tuple[str, str]] = {
    '6.01': ('operating_cash_flow', ''),
    '6.02': ('cfi',                 ''),
    '6.03': ('financing_cash_flow', ''),
}


def sdiv(a, b):
    with np.errstate(divide='ignore', invalid='ignore'):
        return np.where((b != 0) & b.notna() & a.notna(), a / b, np.nan)


# ── Download helpers ───────────────────────────────────────────────────────────

def download_zip(url: str) -> zipfile.ZipFile | None:
    try:
        r = requests.get(url, timeout=60)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return zipfile.ZipFile(io.BytesIO(r.content))
    except Exception as e:
        print(f'    WARN: {url}: {e}')
        return None


def read_csv_from_zip(zf: zipfile.ZipFile, name: str) -> pd.DataFrame | None:
    try:
        return pd.read_csv(zf.open(name), sep=';', encoding='latin1',
                           dtype={'CD_CONTA': str, 'CD_CVM': str})
    except Exception:
        return None


# ── Account extraction ─────────────────────────────────────────────────────────

def extract_accounts(df: pd.DataFrame, code_map: dict) -> pd.DataFrame:
    """
    From a raw CVM statement DataFrame, pivot account codes to columns.
    Returns one row per (CD_CVM, DT_REFER, DT_INI_EXERC, DT_FIM_EXERC).
    """
    if df is None or df.empty:
        return pd.DataFrame()

    # Only take most recent version and last exercise (ÚLTIMO)
    df = df[df.get('ORDEM_EXERC', pd.Series('ÚLTIMO', index=df.index)) == 'ÚLTIMO'].copy()

    # DT_INI_EXERC / DT_FIM_EXERC absent in some early CVM zips
    if 'DT_INI_EXERC' not in df.columns:
        df['DT_INI_EXERC'] = ''
    if 'DT_FIM_EXERC' not in df.columns:
        df['DT_FIM_EXERC'] = ''

    results = []
    for (cd_cvm, dt_refer, dt_ini, dt_fim), grp in df.groupby(
            ['CD_CVM', 'DT_REFER', 'DT_INI_EXERC', 'DT_FIM_EXERC'], sort=False):

        row: dict = {
            'cd_cvm':   str(cd_cvm).zfill(7),
            'dt_refer': str(dt_refer),
            'dt_ini':   str(dt_ini),
            'dt_fim':   str(dt_fim),
            'denom':    grp['DENOM_CIA'].iloc[0] if 'DENOM_CIA' in grp.columns else '',
        }

        code_to_val = dict(zip(grp['CD_CONTA'], grp['VL_CONTA']))
        code_to_ds  = dict(zip(grp['CD_CONTA'], grp['DS_CONTA'].str.lower()))

        for code, (col, kw) in code_map.items():
            val = code_to_val.get(code)
            ds  = code_to_ds.get(code, '')
            if val is not None and (not kw or kw in ds):
                row[col] = float(val)

        results.append(row)

    return pd.DataFrame(results) if results else pd.DataFrame()


def merge_statements(bpa: pd.DataFrame, bpp: pd.DataFrame,
                     dre: pd.DataFrame, dfc: pd.DataFrame) -> pd.DataFrame:
    keys   = ['cd_cvm', 'dt_refer', 'dt_ini', 'dt_fim']
    merged = bpa
    for df in [bpp, dre, dfc]:
        if df is not None and not df.empty:
            df_right   = df.drop(columns=['denom'], errors='ignore')
            merge_cols = [c for c in keys if c in merged.columns and c in df_right.columns]
            merged = merged.merge(df_right, on=merge_cols, how='outer')
    return merged
    return merged


# ── Normalise to schema ────────────────────────────────────────────────────────

def normalise(df: pd.DataFrame, period_type: str, fiscal_quarter: str) -> pd.DataFrame:
    if df.empty:
        return df

    # Scale: CVM reports in thousands (R$ mil) by default
    # Check ESCALA_MOEDA column if present (MIL = thousands, UNIDADE = units)
    # We assume MIL and multiply by 1000 to get absolute values
    numeric_cols = [c for c in df.columns if c not in
                    ['cd_cvm', 'dt_refer', 'dt_ini', 'dt_fim', 'denom',
                     'period_type', 'fiscal_quarter', 'fiscal_year', 'filed_date',
                     'ticker', 'cik', 'market', 'country', 'exchange', 'accounting_std',
                     'currency', 'name']]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce') * 1000  # CVM in R$ thousands

    df['period_type']    = period_type
    df['fiscal_quarter'] = fiscal_quarter
    df['filed_date']     = pd.to_datetime(df['dt_refer'], errors='coerce').dt.strftime('%Y-%m-%d')
    df['fiscal_year']    = pd.to_datetime(df['dt_fim'],   errors='coerce').dt.year.astype(str)
    df['cik']            = df['cd_cvm']
    df['market']         = 'BR'
    df['entity_id']      = 'BR:' + df['cik'].astype(str)
    df['availability_timestamp'] = pd.NaT
    df['availability_provenance'] = 'statement_date_unproven'
    df['country']        = 'BR'
    df['exchange']       = 'B3'
    df['currency']       = 'BRL'
    df['accounting_std'] = 'IFRS'

    # total_liabilities = current + long_term
    cl = df.get('current_liabilities', pd.Series(np.nan, index=df.index))
    lt = df.get('long_term_debt',      pd.Series(np.nan, index=df.index))
    df['total_liabilities'] = (cl.fillna(0) + lt.fillna(0)).replace(0, np.nan)

    df.drop(columns=['cd_cvm', 'dt_refer', 'dt_ini', 'dt_fim'], errors='ignore', inplace=True)
    return df


# ── YoY growth features ────────────────────────────────────────────────────────

def compute_yoy(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(['cik', 'period_type', 'filed_date']).copy()
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
    ]
    for (cik_val, pt), rows in df.groupby(['cik', 'period_type']):
        idx = rows.index
        for src, dst in grow_pairs:
            if src in df.columns:
                prev  = df.loc[idx, src].shift(1)
                curr  = df.loc[idx, src]
                with np.errstate(divide='ignore', invalid='ignore'):
                    growth = np.where(
                        prev.notna() & (prev != 0) & curr.notna(),
                        (curr - prev) / prev.abs(),
                        np.nan,
                    )
                df.loc[idx, dst] = growth
    return df


# ── Process one year zip ───────────────────────────────────────────────────────

def process_annual_zip(year: int) -> pd.DataFrame:
    url = f'{CVM_DFP_BASE}/dfp_cia_aberta_{year}.zip'
    zf  = download_zip(url)
    if zf is None:
        return pd.DataFrame()

    suffix = f'_con_{year}.csv'   # consolidated preferred

    bpa = extract_accounts(read_csv_from_zip(zf, f'dfp_cia_aberta_BPA{suffix}'), BPA_MAP)
    bpp = extract_accounts(read_csv_from_zip(zf, f'dfp_cia_aberta_BPP{suffix}'), BPP_MAP)
    dre = extract_accounts(read_csv_from_zip(zf, f'dfp_cia_aberta_DRE{suffix}'), DRE_MAP)
    dfc = extract_accounts(read_csv_from_zip(zf, f'dfp_cia_aberta_DFC_MI{suffix}'), DFC_MAP)
    if dfc.empty:  # fallback to direct method
        dfc = extract_accounts(read_csv_from_zip(zf, f'dfp_cia_aberta_DFC_MD{suffix}'), DFC_MAP)

    if bpa.empty:
        return pd.DataFrame()

    merged = merge_statements(bpa, bpp, dre, dfc)
    return normalise(merged, 'annual', 'FY')


def process_quarterly_zip(year: int) -> pd.DataFrame:
    url = f'{CVM_ITR_BASE}/itr_cia_aberta_{year}.zip'
    zf  = download_zip(url)
    if zf is None:
        return pd.DataFrame()

    all_frames = []
    for q, months in [('Q1', ['03']), ('Q2', ['06']), ('Q3', ['09'])]:
        suffix = f'_con_{year}.csv'
        bpa = read_csv_from_zip(zf, f'itr_cia_aberta_BPA{suffix}')
        if bpa is None:
            continue
        # Filter to specific quarter by DT_FIM_EXERC month
        for df_raw in [bpa]:
            if df_raw is not None and 'DT_FIM_EXERC' in df_raw.columns:
                df_raw = df_raw[df_raw['DT_FIM_EXERC'].str[5:7].isin(months)]

        bpa_q = extract_accounts(
            bpa[bpa['DT_FIM_EXERC'].str[5:7].isin(months)] if bpa is not None and 'DT_FIM_EXERC' in bpa.columns else bpa,
            BPA_MAP)
        bpp_q = extract_accounts(
            read_csv_from_zip(zf, f'itr_cia_aberta_BPP{suffix}'), BPP_MAP)
        dre_q = extract_accounts(
            read_csv_from_zip(zf, f'itr_cia_aberta_DRE{suffix}'), DRE_MAP)
        dfc_q = extract_accounts(
            read_csv_from_zip(zf, f'itr_cia_aberta_DFC_MI{suffix}'), DFC_MAP)

        if bpa_q.empty:
            continue

        merged = merge_statements(bpa_q, bpp_q, dre_q, dfc_q)
        frame  = normalise(merged, 'quarterly', q)
        if not frame.empty:
            all_frames.append(frame)

    return pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame()


# ── Main ───────────────────────────────────────────────────────────────────────

def run():
    DATA.mkdir(exist_ok=True)

    tickers = pd.read_parquet(TICK) if TICK.exists() else pd.DataFrame()
    valid_ciks = set(tickers['cd_cvm'].astype(str)) if not tickers.empty else None

    print('Step 2 BR — Building CVM financial snapshots')
    print(f'  Annual  : {START_YEAR}–2025')
    print(f'  Quarterly: 2011–2025')

    all_frames = []

    # Annual
    for year in range(START_YEAR, 2026):
        print(f'  Annual {year} ...', end=' ', flush=True)
        df = process_annual_zip(year)
        if df.empty:
            print('no data')
            continue
        if valid_ciks:
            df = df[df['cik'].isin(valid_ciks)]
        print(f'{len(df):,} rows, {df["cik"].nunique():,} companies')
        all_frames.append(df)

    # Quarterly
    for year in range(2011, 2026):
        print(f'  Quarterly {year} ...', end=' ', flush=True)
        df = process_quarterly_zip(year)
        if df.empty:
            print('no data')
            continue
        if valid_ciks:
            df = df[df['cik'].isin(valid_ciks)]
        print(f'{len(df):,} rows')
        all_frames.append(df)

    if not all_frames:
        print('ERROR: no data loaded')
        import sys; sys.exit(1)

    combined = pd.concat(all_frames, ignore_index=True)

    # Merge ticker info
    if not tickers.empty:
        tick_map = tickers[['cd_cvm', 'ticker', 'name', 'industry_code']].rename(
            columns={'cd_cvm': 'cik'})
        combined = combined.merge(tick_map, on='cik', how='left')

    print('\n  Computing YoY growth features ...')
    combined = compute_yoy(combined)

    combined.to_parquet(OUT, index=False)

    print(f'\nStep 2 BR complete.')
    print(f'  Total rows:       {len(combined):,}')
    print(f'  Unique companies: {combined["cik"].nunique():,}')
    print(f'  Annual rows:      {(combined["period_type"]=="annual").sum():,}')
    print(f'  Quarterly rows:   {(combined["period_type"]=="quarterly").sum():,}')
    print(f'  Date range:       {combined["filed_date"].min()} → {combined["filed_date"].max()}')
    print(f'  Saved: {OUT}')


if __name__ == '__main__':
    run()
