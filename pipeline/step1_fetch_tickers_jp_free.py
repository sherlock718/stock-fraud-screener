"""
Step 1 JP Free — Fetch major Japanese listed company tickers (no EDINET key required).

Uses a curated static list of Nikkei 225 + major TSE Prime companies with
yfinance-compatible tickers (format: XXXX.T).

For full coverage of all TSE-listed companies (3,800+), the EDINET-based pipeline
(step1_fetch_tickers_jp.py) provides complete data but requires a free API key.

Output: data/tickers_jp.parquet
  cik, ticker, stock_code, name, exchange, market, country, currency, accounting_std
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

BASE = Path(__file__).parent.parent
DATA = BASE / 'data'
OUT  = DATA / 'tickers_jp.parquet'

# ── Nikkei 225 + major TSE Prime components (yfinance format: XXXX.T) ─────────
# Covers technology, auto, finance, pharma, consumer, industrials, energy, materials

JAPAN_TICKERS = [
    # Technology / Electronics
    ('6758.T', 'Sony Group Corp'),
    ('9984.T', 'SoftBank Group Corp'),
    ('6861.T', 'Keyence Corp'),
    ('8035.T', 'Tokyo Electron Ltd'),
    ('6723.T', 'Renesas Electronics Corp'),
    ('6857.T', 'Advantest Corp'),
    ('6702.T', 'Fujitsu Ltd'),
    ('6501.T', 'Hitachi Ltd'),
    ('6752.T', 'Panasonic Holdings Corp'),
    ('6762.T', 'TDK Corp'),
    ('6981.T', 'Murata Manufacturing Co'),
    ('6645.T', 'Omron Corp'),
    ('6724.T', 'Seiko Epson Corp'),
    ('6841.T', 'Yokogawa Electric Corp'),
    ('6952.T', 'Casio Computer Co'),
    ('6963.T', 'Rohm Co'),
    ('6976.T', 'Taiyo Yuden Co'),
    ('6506.T', 'Yaskawa Electric Corp'),
    ('4704.T', 'Trend Micro Inc'),
    ('4307.T', 'Nomura Research Institute'),
    ('9613.T', 'NTT Data Group Corp'),
    ('3659.T', 'Nexon Co'),
    ('4307.T', 'Nomura Research Institute Ltd'),
    # Automotive
    ('7203.T', 'Toyota Motor Corp'),
    ('7267.T', 'Honda Motor Co'),
    ('7201.T', 'Nissan Motor Co'),
    ('7202.T', 'Isuzu Motors Ltd'),
    ('7211.T', 'Mitsubishi Motors Corp'),
    ('7261.T', 'Mazda Motor Corp'),
    ('7270.T', 'Subaru Corp'),
    ('7272.T', 'Yamaha Motor Co'),
    ('6902.T', 'Denso Corp'),
    ('7011.T', 'Mitsubishi Heavy Industries'),
    ('7012.T', 'Kawasaki Heavy Industries'),
    ('5108.T', 'Bridgestone Corp'),
    ('6471.T', 'NSK Ltd'),
    ('6479.T', 'Minebea Mitsumi Inc'),
    # Telecommunications
    ('9432.T', 'Nippon Telegraph & Telephone'),
    ('9433.T', 'KDDI Corp'),
    ('9437.T', 'NTT Docomo Inc'),
    ('9984.T', 'SoftBank Group Corp'),
    # Finance / Banking
    ('8306.T', 'Mitsubishi UFJ Financial Group'),
    ('8316.T', 'Sumitomo Mitsui Financial Group'),
    ('8411.T', 'Mizuho Financial Group'),
    ('8309.T', 'Sumitomo Mitsui Trust Holdings'),
    ('8604.T', 'Nomura Holdings Inc'),
    ('8601.T', 'Daiwa Securities Group Inc'),
    ('8750.T', 'Dai-ichi Life Insurance Co'),
    ('8766.T', 'Tokio Marine Holdings Inc'),
    ('8725.T', 'MS&AD Insurance Group Holdings'),
    ('8630.T', 'Sompo Holdings Inc'),
    ('7182.T', 'Japan Post Bank Co'),
    # Pharmaceuticals / Healthcare
    ('4502.T', 'Takeda Pharmaceutical Co'),
    ('4519.T', 'Chugai Pharmaceutical Co'),
    ('4523.T', 'Eisai Co'),
    ('4528.T', 'Ono Pharmaceutical Co'),
    ('4578.T', 'Otsuka Holdings Co'),
    ('4151.T', 'Kyowa Kirin Co'),
    ('4543.T', 'Terumo Corp'),
    ('7733.T', 'Olympus Corp'),
    ('4536.T', 'Santen Pharmaceutical Co'),
    ('4506.T', 'Sumitomo Pharma Co'),
    # Consumer / Retail
    ('9983.T', 'Fast Retailing Co'),
    ('3382.T', 'Seven & i Holdings Co'),
    ('8267.T', 'Aeon Co'),
    ('7974.T', 'Nintendo Co'),
    ('4911.T', 'Shiseido Co'),
    ('4452.T', 'Kao Corp'),
    ('4661.T', 'Oriental Land Co'),
    ('7832.T', 'Bandai Namco Holdings Inc'),
    ('2914.T', 'Japan Tobacco Inc'),
    ('2502.T', 'Asahi Group Holdings'),
    ('2503.T', 'Kirin Holdings Co'),
    ('2802.T', 'Ajinomoto Co'),
    ('2269.T', 'Meiji Holdings Co'),
    ('9064.T', 'Yamato Holdings Co'),
    # Industrials / Trading
    ('8001.T', 'Itochu Corp'),
    ('8002.T', 'Marubeni Corp'),
    ('8031.T', 'Mitsui & Co'),
    ('8058.T', 'Mitsubishi Corp'),
    ('8053.T', 'Sumitomo Corp'),
    ('6326.T', 'Kubota Corp'),
    ('6361.T', 'Ebara Corp'),
    ('6503.T', 'Mitsubishi Electric Corp'),
    ('5401.T', 'Nippon Steel Corp'),
    ('5202.T', 'Nippon Sheet Glass Co'),
    ('5201.T', 'AGC Inc'),
    ('5332.T', 'Toto Ltd'),
    ('3407.T', 'Asahi Kasei Corp'),
    ('4063.T', 'Shin-Etsu Chemical Co'),
    ('4188.T', 'Mitsubishi Chemical Group Corp'),
    ('4005.T', 'Sumitomo Chemical Co'),
    ('4183.T', 'Mitsui Chemicals Inc'),
    ('5020.T', 'ENEOS Holdings Inc'),
    ('5019.T', 'Idemitsu Kosan Co'),
    ('5702.T', 'Sumitomo Metal Mining Co'),
    ('5802.T', 'Sumitomo Electric Industries'),
    # Real Estate
    ('8801.T', 'Mitsui Fudosan Co'),
    ('8802.T', 'Mitsubishi Estate Co'),
    ('8830.T', 'Sumitomo Realty & Development Co'),
    ('3289.T', 'Tokyu Fudosan Holdings Corp'),
    # Transport / Logistics
    ('9020.T', 'East Japan Railway Co'),
    ('9022.T', 'Central Japan Railway Co'),
    ('9101.T', 'Nippon Yusen KK'),
    ('9104.T', 'Mitsui OSK Lines'),
    ('9005.T', 'Tokyu Corp'),
    # Utilities
    ('9501.T', 'Tokyo Electric Power Co'),
    ('9503.T', 'Kansai Electric Power Co'),
    ('9531.T', 'Tokyo Gas Co'),
    # Media / Entertainment
    ('9602.T', 'Toho Co'),
    ('7751.T', 'Canon Inc'),
    ('4901.T', 'Fujifilm Holdings Corp'),
    ('6367.T', 'Daikin Industries Ltd'),
    ('7013.T', 'IHI Corp'),
    ('6098.T', 'Recruit Holdings Co'),
    ('7741.T', 'Hoya Corp'),
    ('4021.T', 'Nissan Chemical Corp'),
    ('6146.T', 'Disco Corp'),
    ('6113.T', 'Amada Co'),
    ('6103.T', 'Okuma Corp'),
    ('7956.T', 'Pigeon Corp'),
    ('8252.T', 'Marui Group Co'),
    ('5233.T', 'Taiheiyo Cement Corp'),
    ('5333.T', 'NGK Insulators Ltd'),
    ('6971.T', 'Kyocera Corp'),
    ('4912.T', 'Lion Corp'),
]

# Deduplicate
_seen: set[str] = set()
JAPAN_TICKERS_CLEAN = []
for t, n in JAPAN_TICKERS:
    if t not in _seen:
        _seen.add(t)
        JAPAN_TICKERS_CLEAN.append((t, n))


def run():
    DATA.mkdir(exist_ok=True)
    print('Step 1 JP Free — Building Japanese ticker list (Nikkei 225 + major TSE)')

    records = []
    for ticker, name in JAPAN_TICKERS_CLEAN:
        stock_code = ticker.replace('.T', '')
        records.append({
            'cik':            stock_code,
            'ticker':         ticker,
            'stock_code':     stock_code,
            'name':           name,
            'exchange':       'TSE',
            'market':         'JP',
            'country':        'JP',
            'currency':       'JPY',
            'accounting_std': 'IFRS',
        })

    df = pd.DataFrame(records)
    df.to_parquet(OUT, index=False)

    print(f'\nStep 1 JP Free complete.')
    print(f'  Total companies: {len(df):,}')
    print(f'  Saved: {OUT}')
    print(f'  NOTE: For full TSE coverage (3,800+ companies), register free EDINET key')
    print(f'        at https://disclosure2dl.edinet-fsa.go.jp and run step1_fetch_tickers_jp.py')


if __name__ == '__main__':
    run()
