"""
upstox_downloads.py

All network/download calls to Upstox live here in one place:
  - Instrument master file (for the dynamic F&O universe)
  - Full Market Quotes (for the live scanner polling)

Keeping every download function in one module means:
  - one place to fix rate-limiting / retry / timeout behaviour
  - fno_universe.py and fno_scanner_app.py just import + call, no
    duplicated requests logic
  - easy to add new downloads later (e.g. historical candles for
    RSI/ADX/SMA scoring) without touching the scanner or universe files
"""

import gzip
import json
import time
import urllib.parse
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

INSTRUMENT_MASTER_URL = (
    "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
)
FULL_QUOTE_URL = "https://api.upstox.com/v2/market-quote/quotes"
HISTORICAL_CANDLE_URL = "https://api.upstox.com/v2/historical-candle/{instrument_key}/{interval}/{to_date}/{from_date}"
INTRADAY_CANDLE_URL = "https://api.upstox.com/v2/historical-candle/intraday/{instrument_key}/{interval}"
V3_HISTORICAL_CANDLE_URL = "https://api.upstox.com/v3/historical-candle/{instrument_key}/{unit}/{interval}/{to_date}/{from_date}"

BATCH_SIZE = 500          # Upstox hard limit per quotes request
REQUEST_TIMEOUT = 60
MAX_RETRIES = 2
RETRY_BACKOFF_SECONDS = 1.5
QUOTE_BATCH_WORKERS = 5   # concurrent batch requests -- modest, since each batch is already 500 instruments


def _chunk_list(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def _get_with_retry(url, headers=None, params=None, timeout=REQUEST_TIMEOUT):
    """Shared GET wrapper with basic 429 backoff, used by all downloads below."""
    last_exc = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=timeout)
            if resp.status_code == 429:
                time.sleep(RETRY_BACKOFF_SECONDS * (attempt + 1))
                continue
            if not resp.ok:
                # Upstox includes a specific reason (e.g. "UDAPI100050: Invalid
                # token") in the JSON body -- surface that instead of just the
                # generic "401 Client Error: Unauthorized", it tells you the
                # actual cause (expired vs malformed vs wrong scope) directly.
                try:
                    detail = resp.json()
                except ValueError:
                    detail = resp.text[:300]
                raise requests.HTTPError(f"{resp.status_code} {resp.reason} for {url}\nDetail: {detail}", response=resp)
            return resp
        except requests.RequestException as e:
            last_exc = e
            time.sleep(RETRY_BACKOFF_SECONDS * (attempt + 1))
    raise last_exc


# ---------------------------------------------------------------------------
# 1. Instrument master download (used by fno_universe.py)
# ---------------------------------------------------------------------------
def download_instrument_master():
    """
    Download and parse the full Upstox instrument master (gzip JSON).
    Returns a list of instrument dicts covering every exchange/segment.
    """
    resp = _get_with_retry(INSTRUMENT_MASTER_URL)
    raw = gzip.decompress(resp.content)
    return json.loads(raw)


# ---------------------------------------------------------------------------
# 2. Full market quotes download (used by fno_scanner_app.py)
# ---------------------------------------------------------------------------
def _fetch_quote_batch(batch, headers):
    params = {"instrument_key": ",".join(batch)}
    resp = _get_with_retry(FULL_QUOTE_URL, headers=headers, params=params, timeout=15)
    return resp.json().get("data", {})


def download_full_quotes(instrument_keys, access_token):
    """
    Fetch Full Market Quotes for a list of instrument_keys, batched at 500
    per request (Upstox's hard limit), with batches fetched CONCURRENTLY
    rather than one after another. This matters once the cash-market
    universe is in play -- futures (~209) + full cash market (~2000) can
    mean ~5 batches per poll cycle; running them sequentially risked
    eating into the 10-second poll interval itself, especially as the
    universe grows. Returns the merged {instrument_key: quote} dict.
    """
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {access_token}",
    }
    batches = list(_chunk_list(instrument_keys, BATCH_SIZE))
    all_data = {}

    if len(batches) <= 1:
        # Single batch -- no need for thread pool overhead.
        for batch in batches:
            all_data.update(_fetch_quote_batch(batch, headers))
        return all_data

    with ThreadPoolExecutor(max_workers=QUOTE_BATCH_WORKERS) as executor:
        futures = [executor.submit(_fetch_quote_batch, batch, headers) for batch in batches]
        for future in as_completed(futures):
            all_data.update(future.result())
    return all_data


# ---------------------------------------------------------------------------
# 3. Historical candles download (for future RSI/ADX/SMA scoring)
# ---------------------------------------------------------------------------
def download_historical_candles(instrument_key, interval, from_date, to_date, access_token):
    """
    Fetch historical OHLC(+volume+OI) candles for one instrument.
    interval: 'day' | '30minute' | '15minute' | '5minute' | '3minute' | '1minute'
    from_date / to_date: 'YYYY-MM-DD'

    Returns candles newest-first, each as [timestamp, open, high, low, close, volume, oi].
    """
    headers = {"Accept": "application/json", "Authorization": f"Bearer {access_token}"}
    # instrument_key contains a literal '|' (e.g. "NSE_FO|12345") which MUST be
    # percent-encoded when placed directly in the URL path -- unlike query params
    # (which `requests` encodes automatically), a raw '|' in the path was silently
    # breaking every one of these calls and made reference/SMA data come back empty.
    safe_key = urllib.parse.quote(instrument_key, safe="")
    url = HISTORICAL_CANDLE_URL.format(
        instrument_key=safe_key, interval=interval, to_date=to_date, from_date=from_date
    )
    resp = _get_with_retry(url, headers=headers, timeout=30)
    return resp.json().get("data", {}).get("candles", [])


def download_intraday_candles(instrument_key, interval, access_token):
    """
    Fetch TODAY's candles so far (the completed historical-candle endpoint
    only covers up through yesterday). Same candle format as above.
    """
    headers = {"Accept": "application/json", "Authorization": f"Bearer {access_token}"}
    safe_key = urllib.parse.quote(instrument_key, safe="")
    url = INTRADAY_CANDLE_URL.format(instrument_key=safe_key, interval=interval)
    resp = _get_with_retry(url, headers=headers, timeout=30)
    return resp.json().get("data", {}).get("candles", [])


def download_v3_intraday_candles(instrument_key, to_date, from_date, access_token, unit="minutes", interval=5):
    """
    v3 historical-candle endpoint, e.g.
    https://api.upstox.com/v3/historical-candle/{instrument_key}/minutes/5/{to_date}/{from_date}
    This is the exact pattern your existing morning_prep_banknifty.py uses
    to build the RVOL baseline (last 10 trading days of 5-min candles,
    grouped by date). Returns candles sorted oldest-first, each as
    [timestamp_iso, open, high, low, close, volume, oi].
    """
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    safe_key = urllib.parse.quote(instrument_key, safe="")
    url = V3_HISTORICAL_CANDLE_URL.format(
        instrument_key=safe_key, unit=unit, interval=interval, to_date=to_date, from_date=from_date
    )
    resp = _get_with_retry(url, headers=headers, timeout=30)
    candles = resp.json().get("data", {}).get("candles", [])
    candles.sort(key=lambda c: c[0])
    return candles


if __name__ == "__main__":
    # Quick smoke test for the instrument master download only
    # (quotes/candles need a live access token, so not tested here)
    print("Downloading instrument master...")
    instruments = download_instrument_master()
    print(f"Downloaded {len(instruments)} instruments.")
