"""
cash_scanner_app.py

Standalone cash-market scanner -- tracks the ENTIRE NSE cash/equity
market (~2000+ stocks), ranked by relative volume (RVOL) vs. a
time-of-day-matched 10-day baseline, alongside VWAP. Same proven
methodology as the futures scanner, but this app has no F&O dependency
at all -- pure cash market.

Run:
    streamlit run cash_scanner_app.py

Requires:
    UPSTOX_ACCESS_TOKEN env var locally, or ACCESS_TOKEN in Streamlit
    Cloud Secrets, or paste into the sidebar at runtime.
"""

import os
import time
import threading
import pandas as pd
import streamlit as st

from cash_universe import load_cash_universe
from cash_rvol_baseline import load_cash_baselines, rvol as compute_rvol
from upstox_downloads import download_full_quotes, BATCH_SIZE

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
POLL_INTERVAL_SECONDS = 10

st.set_page_config(page_title="Cash Market Scanner", layout="wide")


# ---------------------------------------------------------------------------
# Background polling worker
# ---------------------------------------------------------------------------
class ScannerState:
    """
    Thread-safe container for the latest scan results.
    Plain dict + lock (not st.session_state) inside the background thread
    avoids touching Streamlit state from a non-main thread.
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.df = pd.DataFrame()
        self.last_update = None
        self.last_error = None
        self.running = False
        self.leader_log = []
        self.current_leader = None

    def set_result(self, df):
        with self.lock:
            self.df = df
            self.last_update = time.time()
            self.last_error = None

            if not df.empty:
                top_row = df.iloc[0]
                top_symbol = top_row["Symbol"]
                if top_symbol != self.current_leader:
                    self.current_leader = top_symbol
                    self.leader_log.append({
                        "Time": time.strftime("%H:%M:%S", time.localtime(self.last_update)),
                        "Symbol": top_symbol,
                        "Vol Chg %": top_row["Vol Chg %"],
                        "LTP": top_row["LTP"],
                        "VWAP": top_row["VWAP"],
                    })

    def set_error(self, msg):
        with self.lock:
            self.last_error = msg

    def get(self):
        with self.lock:
            return self.df.copy(), self.last_update, self.last_error, list(self.leader_log)


def score_row(quote, baseline):
    """Same scoring logic as the futures scanner -- pure volume% + VWAP."""
    ltp = quote.get("last_price", 0) or 0
    volume = quote.get("volume", 0) or 0
    vwap = quote.get("average_price", 0) or 0

    net_change = quote.get("net_change", 0) or 0
    prev_close = ltp - net_change
    pct_change = (net_change / prev_close * 100) if prev_close else 0

    rvol_value = compute_rvol(volume, baseline)
    vol_change_pct = (rvol_value - 1) * 100 if rvol_value is not None else None

    vwap_side = "Above VWAP" if ltp > vwap else ("Below VWAP" if ltp < vwap else "At VWAP")

    return {
        "LTP": round(ltp, 2),
        "% Chg": round(pct_change, 2),
        "RVOL": round(rvol_value, 2) if rvol_value is not None else None,
        "Vol Chg %": round(vol_change_pct, 2) if vol_change_pct is not None else None,
        "VWAP": round(vwap, 2),
        "vs VWAP": vwap_side,
    }


def poll_loop(state: ScannerState, cash_universe: dict, baselines: dict, access_token: str, stop_event: threading.Event):
    state.running = True
    while not stop_event.is_set():
        try:
            quotes = download_full_quotes(list(cash_universe.values()), access_token)

            # Match by each quote's own "instrument_token" field, which
            # uses our instrument_key format ("NSE_EQ|INE..."), NOT the
            # response dict's own "EXCHANGE:TRADINGSYMBOL" keys.
            quotes_by_token = {q.get("instrument_token"): q for q in quotes.values()}

            rows = []
            for symbol, instrument_key in cash_universe.items():
                quote = quotes_by_token.get(instrument_key)
                if quote is None:
                    continue
                display_symbol = quote.get("symbol", symbol)
                row = {"Symbol": display_symbol}
                row.update(score_row(quote, baselines.get(symbol, {})))
                rows.append(row)

            df = pd.DataFrame(rows).sort_values("Vol Chg %", ascending=False, na_position="last").reset_index(drop=True)
            if not df.empty:
                df.insert(0, "Sl No", range(1, len(df) + 1))
            state.set_result(df)

        except Exception as e:
            state.set_error(str(e))

        stop_event.wait(POLL_INTERVAL_SECONDS)
    state.running = False


def get_default_token():
    try:
        if "ACCESS_TOKEN" in st.secrets:
            return st.secrets["ACCESS_TOKEN"]
    except Exception:
        pass
    return os.environ.get("UPSTOX_ACCESS_TOKEN", "")


# ---------------------------------------------------------------------------
# Streamlit UI
# ---------------------------------------------------------------------------
def main():
    st.title("Cash Market Scanner — Full NSE Universe")

    with st.sidebar:
        st.header("Setup")
        default_token = get_default_token()
        access_token = st.text_input(
            "Upstox Access Token",
            value=default_token,
            type="password",
            help="Set UPSTOX_ACCESS_TOKEN env var locally, or ACCESS_TOKEN in "
                 "Streamlit Cloud Secrets, to avoid pasting this every run.",
        )
        access_token = access_token.strip() if access_token else access_token
        refresh_universe = st.button("Refresh universe now")
        st.caption(f"Polling every {POLL_INTERVAL_SECONDS}s · batches of {BATCH_SIZE}")

    if not access_token:
        st.warning("Enter your Upstox access token in the sidebar to start scanning.")
        st.stop()

    cash_universe = load_cash_universe(force_refresh=refresh_universe)
    st.caption(f"Tracking all {len(cash_universe)} NSE cash-market stocks.")

    # See fno_scanner_app.py's comments for why this dict is mutated in
    # place rather than reassigned: the background thread holds a
    # reference to this exact object for its whole life.
    if "cash_baselines" not in st.session_state:
        st.session_state.cash_baselines = {}
        st.session_state.baselines_built = False

    if not st.session_state.baselines_built or refresh_universe:
        progress_bar = st.progress(0.0, text="Building RVOL baselines (last 10 trading days) — this covers ~2000 stocks, expect several minutes...")

        def _progress(i, total, symbol):
            progress_bar.progress(i / total, text=f"Building RVOL baselines... {i}/{total} ({symbol})")

        fresh_baselines, sample_error, error_count = load_cash_baselines(
            cash_universe, access_token, force_refresh=refresh_universe, progress_callback=_progress
        )
        st.session_state.cash_baselines.clear()
        st.session_state.cash_baselines.update(fresh_baselines)
        st.session_state.baselines_built = True
        progress_bar.empty()

        if sample_error:
            st.warning(
                f"RVOL baseline: {error_count}/{len(cash_universe)} stocks failed to fetch. "
                f"Sample error — {sample_error}"
            )

    baselines = st.session_state.cash_baselines

    if "scanner_state" not in st.session_state:
        st.session_state.scanner_state = ScannerState()
        st.session_state.stop_event = threading.Event()
        thread = threading.Thread(
            target=poll_loop,
            args=(st.session_state.scanner_state, cash_universe, baselines, access_token, st.session_state.stop_event),
            daemon=True,
        )
        thread.start()
        st.session_state.scanner_thread = thread

    state: ScannerState = st.session_state.scanner_state
    df, last_update, error, leader_log = state.get()

    status_col, _ = st.columns([3, 1])
    with status_col:
        if error:
            st.error(f"Last poll error: {error}")
        elif last_update:
            st.success(f"Last updated: {time.strftime('%H:%M:%S', time.localtime(last_update))} IST")
        else:
            st.info("Waiting for first poll...")

    if not df.empty:
        leader = df.iloc[0]
        st.metric(
            label="Current volume leader",
            value=str(leader["Symbol"]),
            delta=f"{leader['Vol Chg %']}% vs 10-day avg · {leader['vs VWAP']}",
        )

        st.subheader("Ranked by Vol Chg % (highest first)")
        st.dataframe(df, use_container_width=True, height=600)

        if leader_log:
            st.subheader("Leadership timeline")
            st.caption("Logged every time a new stock takes the #1 spot by Vol Chg %.")
            log_df = pd.DataFrame(leader_log[::-1])
            st.dataframe(log_df, use_container_width=True, height=250)
    else:
        st.info("No data yet — first poll can take a few seconds.")

    time.sleep(2)
    st.rerun()


if __name__ == "__main__":
    main()
