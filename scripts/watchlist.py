"""
Watchlist & price-trigger alert system.

Persist tickers with optional price targets to data/watchlist.json.
Fetch live prices via yfinance and surface triggered alerts.

Usage:
    python3 scripts/watchlist.py add AAPL --above 200 --below 150 --note "QEM pick"
    python3 scripts/watchlist.py remove AAPL
    python3 scripts/watchlist.py list
    python3 scripts/watchlist.py check          # fetch prices, show alerts
    python3 scripts/watchlist.py export         # write data/watchlist_live.json for frontend
"""
from __future__ import annotations
import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import yfinance as yf

BASE     = Path(__file__).parent.parent
WL_PATH  = BASE / 'data' / 'watchlist.json'
LIVE_OUT = BASE / 'data' / 'watchlist_live.json'


# ── Persistence ───────────────────────────────────────────────────────────────

def _load() -> dict:
    if WL_PATH.exists():
        return json.loads(WL_PATH.read_text())
    return {}


def _save(wl: dict) -> None:
    WL_PATH.write_text(json.dumps(wl, indent=2))


# ── Commands ──────────────────────────────────────────────────────────────────

def cmd_add(ticker: str, above: float | None, below: float | None, note: str) -> None:
    wl = _load()
    wl[ticker.upper()] = {
        'added':    datetime.utcnow().strftime('%Y-%m-%d'),
        'above':    above,
        'below':    below,
        'note':     note,
    }
    _save(wl)
    print(f"Added {ticker.upper()} | above={above} | below={below} | note={note!r}")


def cmd_remove(ticker: str) -> None:
    wl = _load()
    if ticker.upper() not in wl:
        print(f"{ticker.upper()} not in watchlist")
        return
    del wl[ticker.upper()]
    _save(wl)
    print(f"Removed {ticker.upper()}")


def cmd_list() -> None:
    wl = _load()
    if not wl:
        print("Watchlist is empty.")
        return
    print(f"{'Ticker':<10} {'Added':<12} {'Above':>10} {'Below':>10}  Note")
    print('-' * 60)
    for tk, v in sorted(wl.items()):
        ab = f"${v['above']:,.2f}" if v.get('above') else '-'
        bl = f"${v['below']:,.2f}" if v.get('below') else '-'
        print(f"{tk:<10} {v['added']:<12} {ab:>10} {bl:>10}  {v.get('note','')}")


def _fetch_prices(tickers: list[str]) -> dict[str, float | None]:
    prices: dict[str, float | None] = {}
    if not tickers:
        return prices
    raw = yf.download(tickers, period='1d', progress=False, auto_adjust=True)
    if 'Close' in raw.columns:
        last = raw['Close'].iloc[-1]
        for tk in tickers:
            val = last.get(tk) if hasattr(last, 'get') else (last if len(tickers) == 1 else None)
            prices[tk] = float(val) if val is not None and not (hasattr(val, '__class__') and val.__class__.__name__ == 'float' and val != val) else None
    # fallback for single ticker
    if len(tickers) == 1 and not prices:
        tk = tickers[0]
        try:
            info = yf.Ticker(tk).fast_info
            prices[tk] = float(info.last_price) if info.last_price else None
        except Exception:
            prices[tk] = None
    return prices


def cmd_check(quiet: bool = False) -> list[dict]:
    wl = _load()
    if not wl:
        print("Watchlist is empty.")
        return []

    tickers = list(wl.keys())
    print(f"Fetching prices for {len(tickers)} tickers...")
    prices = _fetch_prices(tickers)

    alerts = []
    rows = []
    for tk, meta in sorted(wl.items()):
        price = prices.get(tk)
        triggered = []
        if price is not None:
            if meta.get('above') and price >= meta['above']:
                triggered.append(f"ABOVE ${meta['above']:,.2f}")
            if meta.get('below') and price <= meta['below']:
                triggered.append(f"BELOW ${meta['below']:,.2f}")
        alert_str = ' | '.join(triggered) if triggered else ''
        if triggered:
            alerts.append({'ticker': tk, 'price': price, 'trigger': alert_str})
        rows.append({
            'ticker': tk, 'price': price,
            'above':  meta.get('above'),
            'below':  meta.get('below'),
            'note':   meta.get('note', ''),
            'added':  meta.get('added', ''),
            'alert':  alert_str,
        })

    if not quiet:
        print(f"\n{'Ticker':<10} {'Price':>10} {'Above':>10} {'Below':>10}  Alert")
        print('-' * 65)
        for r in rows:
            p  = f"${r['price']:,.2f}"  if r['price']  else 'N/A'
            ab = f"${r['above']:,.2f}"  if r['above']  else '-'
            bl = f"${r['below']:,.2f}"  if r['below']  else '-'
            flag = '  *** ALERT ***' if r['alert'] else ''
            print(f"{r['ticker']:<10} {p:>10} {ab:>10} {bl:>10}  {r['alert']}{flag}")

        if alerts:
            print(f"\n{'!'*50}")
            print(f"  {len(alerts)} ALERT(S) TRIGGERED")
            for a in alerts:
                print(f"  {a['ticker']}: ${a['price']:,.2f} → {a['trigger']}")
            print(f"{'!'*50}")
        else:
            print("\nNo alerts triggered.")

    return rows


def cmd_export() -> None:
    rows = cmd_check(quiet=True)
    out = {
        'generated_at': datetime.utcnow().isoformat() + 'Z',
        'items': rows,
        'alerts': [r for r in rows if r.get('alert')],
    }
    LIVE_OUT.write_text(json.dumps(out, indent=2))
    print(f"Exported {len(rows)} tickers → {LIVE_OUT}")
    if out['alerts']:
        print(f"  {len(out['alerts'])} alert(s) triggered:")
        for a in out['alerts']:
            print(f"    {a['ticker']}: ${a['price']:,.2f} → {a['alert']}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description='Watchlist + price-trigger alerts')
    sub = parser.add_subparsers(dest='cmd')

    p_add = sub.add_parser('add', help='Add ticker to watchlist')
    p_add.add_argument('ticker')
    p_add.add_argument('--above', type=float, default=None, help='Alert when price >= this')
    p_add.add_argument('--below', type=float, default=None, help='Alert when price <= this')
    p_add.add_argument('--note',  default='', help='Free-text note')

    p_rm = sub.add_parser('remove', help='Remove ticker from watchlist')
    p_rm.add_argument('ticker')

    sub.add_parser('list',   help='Print watchlist')
    sub.add_parser('check',  help='Fetch live prices and show alerts')
    sub.add_parser('export', help='Write watchlist_live.json for frontend')

    args = parser.parse_args()

    if args.cmd == 'add':
        cmd_add(args.ticker, args.above, args.below, args.note)
    elif args.cmd == 'remove':
        cmd_remove(args.ticker)
    elif args.cmd == 'list':
        cmd_list()
    elif args.cmd == 'check':
        cmd_check()
    elif args.cmd == 'export':
        cmd_export()
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()
