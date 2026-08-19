"""
cash_universe.py

Builds the list of ALL NSE-listed cash/equity stocks (~2000+) using the
Upstox instrument master file. Standalone version -- no F&O/futures logic,
just the full cash market.

Source: https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz

Usage:
    python cash_universe.py              # builds/refreshes cash_universe.json
    from cash_universe import load_cash_universe
    universe = load_cash_universe()      # {trading_symbol: instrument_key}
"""

import os
import json
import time

from upstox_downloads import download_instrument_master, INSTRUMENT_MASTER_URL

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_FILE = os.path.join(BASE_DIR, "cash_universe.json")
CACHE_MAX_AGE_SECONDS = 24 * 60 * 60  # refresh once a day


def _build_cash_universe(instruments):
    """
    Every NSE-listed cash/equity stock -- {trading_symbol: instrument_key}.
    No F&O filtering; this is the full market.
    """
    equity_lookup = {}
    for inst in instruments:
        if inst.get("segment") == "NSE_EQ" and inst.get("instrument_type") == "EQ":
            trading_symbol = inst.get("trading_symbol")
            instrument_key = inst.get("instrument_key")
            if trading_symbol and instrument_key:
                equity_lookup[trading_symbol] = instrument_key
    return equity_lookup


def refresh_cash_universe():
    """Force a fresh download + rebuild of the cash universe cache."""
    print(f"[cash_universe] Downloading instrument master from {INSTRUMENT_MASTER_URL} ...")
    instruments = download_instrument_master()
    print(f"[cash_universe] Parsed {len(instruments)} total instruments.")

    universe = _build_cash_universe(instruments)
    print(f"[cash_universe] Resolved {len(universe)} NSE cash-market stocks.")

    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump({"generated_at": time.time(), "universe": universe}, f, indent=2)
    print(f"[cash_universe] Cached to {CACHE_FILE}")
    return universe


def load_cash_universe(force_refresh=False):
    """
    Load the full cash universe, using the local cache if fresh enough.
    Returns {trading_symbol: instrument_key}.
    """
    if not force_refresh and os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            cached = json.load(f)
        age = time.time() - cached.get("generated_at", 0)
        if age < CACHE_MAX_AGE_SECONDS:
            return cached["universe"]
        print("[cash_universe] Cache is stale, refreshing...")

    return refresh_cash_universe()


if __name__ == "__main__":
    universe = refresh_cash_universe()
    print(f"\nSample (first 10): {list(universe.items())[:10]}")
