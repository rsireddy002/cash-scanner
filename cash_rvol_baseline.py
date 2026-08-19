"""
cash_rvol_baseline.py

Builds the RVOL baseline for the full cash market -- same proven
time-of-day-matched methodology as the futures scanner:

  For each 5-minute bucket (09:15, 09:20, ..., 15:30), average the
  CUMULATIVE volume that had been traded by that time of day, across the
  last 10 trading sessions. RVOL = today's cumulative volume so far /
  the baseline's value at the nearest earlier bucket.

Standalone version -- no futures fallback logic, just cash/equity
instruments directly.
"""

import os
import json
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from upstox_downloads import download_v3_intraday_candles

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_FILE = os.path.join(BASE_DIR, "cash_rvol_baseline.json")
CACHE_MAX_AGE_SECONDS = 24 * 60 * 60  # the baseline curve barely moves day to day -- refresh once daily

INTRADAY_LOOKBACK_DAYS = 10
FETCH_BUFFER_DAYS = 5  # pull a few extra calendar days to comfortably cover 10 TRADING days
MAX_WORKERS = 8
MIN_BUCKETS_FOR_USABLE_BASELINE = 20  # below this, treat as "too sparse" (e.g. a recent listing)


def _fetch_baseline(instrument_key, access_token):
    """Same algorithm as your proven morning_prep_banknifty.py's build_rvol_baseline()."""
    to_date = datetime.now().strftime("%Y-%m-%d")
    from_date = (datetime.now() - timedelta(days=INTRADAY_LOOKBACK_DAYS + FETCH_BUFFER_DAYS)).strftime("%Y-%m-%d")

    candles = download_v3_intraday_candles(instrument_key, to_date, from_date, access_token)
    if not candles:
        return {}

    today_str = datetime.now().strftime("%Y-%m-%d")

    by_date = {}
    for c in candles:
        ts = c[0]  # ISO timestamp string, e.g. "2026-08-14T09:15:00+05:30"
        date_str = ts[:10]
        if date_str == today_str:
            continue  # exclude today -- may be partial, would skew the baseline
        by_date.setdefault(date_str, []).append(c)

    recent_dates = sorted(by_date.keys())[-INTRADAY_LOOKBACK_DAYS:]

    cumulative_by_date = {}
    for date_str in recent_dates:
        day_candles = sorted(by_date[date_str], key=lambda c: c[0])
        cum_vol = 0
        bucket_map = {}
        for c in day_candles:
            time_str = c[0][11:16]  # "HH:MM"
            cum_vol += c[5]  # volume is index 5
            bucket_map[time_str] = cum_vol
        cumulative_by_date[date_str] = bucket_map

    all_buckets = sorted({t for bm in cumulative_by_date.values() for t in bm.keys()})
    baseline = {}
    for bucket in all_buckets:
        values = [
            cumulative_by_date[d][bucket]
            for d in cumulative_by_date
            if bucket in cumulative_by_date[d]
        ]
        if values:
            baseline[bucket] = round(sum(values) / len(values), 2)

    return baseline


def rvol(cum_volume_today, baseline):
    """
    Compares current cumulative volume to the baseline curve's value at
    the nearest earlier time bucket. Returns None if there's no usable
    baseline yet (e.g. a recently listed stock).
    """
    if not baseline:
        return None
    now_str = datetime.now().strftime("%H:%M")
    matched = None
    for b in sorted(baseline.keys()):
        if b <= now_str:
            matched = b
        else:
            break
    if matched is None or baseline.get(matched, 0) == 0:
        return None
    return cum_volume_today / baseline[matched]


def refresh_cash_baselines(cash_universe, access_token, progress_callback=None):
    """
    Force a fresh build of the cash-market RVOL baseline cache. This is
    the slow part -- ~2000+ stocks, one API call each, fetched
    concurrently (MAX_WORKERS at a time). Expect several minutes even
    with concurrency; it's cached for a full day afterward.

    After the main pass, any symbol that came back EMPTY gets one retry
    at lower concurrency. At this scale (~2637 concurrent-ish requests),
    some failures are transient (rate-limiting, timeouts) rather than a
    genuine lack of data -- without this, a perfectly liquid stock like
    TECHM or BLUEDART could end up with no baseline just from bad timing,
    indistinguishable from an illiquid stock that legitimately has none.
    """
    baselines = {}
    sample_error = None
    error_count = 0
    items = list(cash_universe.items())
    total = len(items)
    completed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_symbol = {
            executor.submit(_fetch_baseline, instrument_key, access_token): symbol
            for symbol, instrument_key in items
        }
        for future in as_completed(future_to_symbol):
            symbol = future_to_symbol[future]
            try:
                baselines[symbol] = future.result()
            except Exception as e:
                baselines[symbol] = {}
                error_count += 1
                if sample_error is None:
                    sample_error = f"{symbol}: {type(e).__name__}: {e}"
                print(f"[cash_rvol_baseline] {symbol}: failed to fetch ({e})")

            completed += 1
            if progress_callback:
                progress_callback(completed, total, symbol)

    # Retry pass: anything that came back empty gets one more attempt,
    # at lower concurrency, to separate "genuinely no data" from
    # "got rate-limited during the big concurrent rush."
    empty_symbols = [s for s, b in baselines.items() if len(b) == 0]
    if empty_symbols:
        print(f"[cash_rvol_baseline] Retrying {len(empty_symbols)} symbols that came back empty...")
        symbol_to_key = {s: k for s, k in items}
        retry_recovered = 0
        with ThreadPoolExecutor(max_workers=max(1, MAX_WORKERS // 2)) as executor:
            future_to_symbol = {
                executor.submit(_fetch_baseline, symbol_to_key[s], access_token): s
                for s in empty_symbols
            }
            for i, future in enumerate(as_completed(future_to_symbol), start=1):
                symbol = future_to_symbol[future]
                try:
                    result = future.result()
                    if result:
                        baselines[symbol] = result
                        retry_recovered += 1
                except Exception as e:
                    print(f"[cash_rvol_baseline] retry {symbol}: still failed ({e})")
                if progress_callback:
                    progress_callback(total, total, f"retry {i}/{len(empty_symbols)}")
        print(f"[cash_rvol_baseline] Retry recovered {retry_recovered}/{len(empty_symbols)} symbols.")

    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump({"generated_at": time.time(), "baselines": baselines}, f, indent=2)

    still_empty = sum(1 for b in baselines.values() if len(b) == 0)
    print(f"[cash_rvol_baseline] Cached RVOL baselines for {len(baselines)} symbols "
          f"({error_count} initial failures, {still_empty} still empty after retry).")
    return baselines, sample_error, error_count


def load_cash_baselines(cash_universe, access_token, force_refresh=False, progress_callback=None):
    """
    Load the cash-market baseline cache, rebuilding if stale, missing,
    covering a different universe, or entirely empty/sparse (self-healing
    against a bad prior build, same as the futures scanner).
    """
    if not force_refresh and os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            cached = json.load(f)
        age = time.time() - cached.get("generated_at", 0)
        cached_baselines = cached.get("baselines", {})
        usable_count = sum(
            1 for b in cached_baselines.values() if len(b) >= MIN_BUCKETS_FOR_USABLE_BASELINE
        )
        cache_is_useless = cached_baselines and usable_count == 0
        if age < CACHE_MAX_AGE_SECONDS and set(cash_universe) <= set(cached_baselines) and not cache_is_useless:
            return cached_baselines, None, 0
        if cache_is_useless:
            print("[cash_rvol_baseline] Cache is all empty/sparse -- treating as stale, refreshing...")
        else:
            print("[cash_rvol_baseline] Cache stale or missing symbols, refreshing...")

    return refresh_cash_baselines(cash_universe, access_token, progress_callback)
