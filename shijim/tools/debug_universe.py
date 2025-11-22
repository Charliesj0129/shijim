import argparse
import logging
import os
from typing import Any

import shioaji as sj
from shijim.gateway.navigator import UniverseNavigator


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', default=os.getenv('SHIOAJI_SIMULATION', 'live'))
    parser.add_argument('--limit', type=int, default=200)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    api = sj.Shioaji()
    api.login(api_key=os.getenv('SHIOAJI_API_KEY', ''), secret_key=os.getenv('SHIOAJI_SECRET_KEY', ''), contracts_timeout=10000)
    navigator = UniverseNavigator(api=api, logger=logging.getLogger('debug_universe'))
    universe = navigator.select_universe(['top_volume'], limit=args.limit)
    print(f"Loaded {len(universe.stocks)} stocks, {len(universe.futures)} futures")
    # show first 20 codes
    print('Sample stocks:', universe.stocks[:20])

if __name__ == '__main__':
    main()
