from __future__ import annotations

import os
from pathlib import Path

BASE       = Path(__file__).parent.parent
DATA_PATH  = BASE / 'data' / 'historical_dataset_clean.parquet'
META_PATH  = BASE / 'models' / 'model_meta.json'
MODELS_DIR = BASE / 'models'

HF_REPO  = os.environ.get('HF_REPO', '')
_IS_CLOUD = bool(HF_REPO) or not DATA_PATH.parent.exists()

BT_PATH      = BASE / 'data' / 'backtest_results.json'
WL_PATH      = BASE / 'data' / 'watchlist.json'
WL_LIVE      = BASE / 'data' / 'watchlist_live.json'
REFRESH_PATH = BASE / 'data' / 'refresh_status.json'
SECTOR_PATH  = BASE / 'data' / 'sector_dividend_map.parquet'

STRAT_FILES = {
    'QEM — Quality + Earnings Momentum': BASE / 'data' / 'strategy_qem.csv',
    'SCDV — Small-Cap Deep Value':        BASE / 'data' / 'strategy_scdv.csv',
    'IARB — International Arbitrage':     BASE / 'data' / 'strategy_iarb.csv',
}

MARKET_LABELS = {
    'US': '🇺🇸 United States', 'CA': '🇨🇦 Canada', 'BR': '🇧🇷 Brazil',
    'JP': '🇯🇵 Japan', 'DE': '🇩🇪 Germany', 'FR': '🇫🇷 France',
    'IT': '🇮🇹 Italy', 'ES': '🇪🇸 Spain', 'SE': '🇸🇪 Sweden',
    'FI': '🇫🇮 Finland', 'NL': '🇳🇱 Netherlands', 'PT': '🇵🇹 Portugal',
    'DK': '🇩🇰 Denmark',
}
