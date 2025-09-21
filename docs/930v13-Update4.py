import requests
import time
from datetime import datetime, timedelta
import pytz
import pandas as pd
import numpy as np
from collections import deque
import csv
import uuid
import sys
import os
#
# === CONFIGURATION ===
# --- Ratcheting stop (step-ladder) ---
ENABLE_RATCHET_STOPS = True
# Start ratchets immediately, independent of BE gate (set True to wait for BE)
RATCHET_REQUIRE_BE = True

# --- Execution pacing ---
ALLOW_MULTI_PARTIALS_PER_BAR = False   # True = allow P1+P2 in same bar; False = only one partial per bar


# Step levels for LONG: when move ≥ trigger, set stop to entry + lock_pct
STEP_RATCHET_LEVELS_LONG = [
    #{"trigger_move_pct": 0.02, "lock_from_entry_pct": 0.00},  # BE at +2%
    {"trigger_move_pct": 0.05, "lock_from_entry_pct": 0.02},  # lock +2% at +5% move
    {"trigger_move_pct": 0.08, "lock_from_entry_pct": 0.04},  # lock +4% at +8%
    {"trigger_move_pct": 0.12, "lock_from_entry_pct": 0.08},  # lock +8% at +12%
]

# Step levels for SHORT: symmetrical
STEP_RATCHET_LEVELS_SHORT = [
    #{"trigger_move_pct": 0.02, "lock_from_entry_pct": 0.00},  # BE at -2% move
    {"trigger_move_pct": 0.05, "lock_from_entry_pct": 0.02},  # lock +2% at -5% move
    {"trigger_move_pct": 0.08, "lock_from_entry_pct": 0.04},  # lock +4% at -8%
    {"trigger_move_pct": 0.12, "lock_from_entry_pct": 0.08},  # lock +8% at -12%
]
API_KEY        = 'Pe53cvMrDBhcXms6202OkhBksvn3Ug7c'
CSV_FILE       = 'tickers.csv'
ENTRY_TIME_ET  = '09:30:00'  # Time only - date comes from CSV
TIMEZONE       = 'US/Eastern'
REPLAY_DELAY   = 0.0     # seconds between ticks (0=as fast as possible)
EQUITY         = 200_000
# Show daily direction P&L summary beneath === TRADE LOGS ===
SHOW_COHORT_20_30 = True
COHORT_20_30_WINDOW = (20.0, 30.0)   # [min_low, min_high)
SHOW_DAILY_DIR_PNL = True   # Show daily P&L summary by direction
SHOW_CONFIG_SUMMARY = True  # Show config summary at end
SHOW_RESULT_TABLES     = True   # Show extended result tables
SHOW_DIRECTION_SUMMARY = True   # Show direction summary table
SHOW_EXIT_BREAKDOWN    = True  # Show Exit Reason × Outcome

# === DATA SOURCE TOGGLE ===
# 'polygon' => fetch minute bars via Polygon REST
# 'databento_local' => read 1s OHLCV from data_db and run on 1s bars (no resample for trading)
DATA_SOURCE    = 'databento_local'   # 'polygon' | 'databento_local'
DATA_DB_DIR    = 'data_db'
DB_DATASET     = 'EQUS-MINI'
DB_SCHEMA      = 'ohlcv-1s'
DB_PREFERRED_EXT = 'parquet'         # try parquet first, then csv

# Polygon interval config (Polygon path only)
INTERVAL_MULTIPLIER = 1
INTERVAL_TIMESPAN   = 'minute'

# Entry window configuration
entryWindowMinutes = 30

# Debug and Logging Levels
LOG_LEVEL      = 4  # 0=off, 1=essential, 2=detailed, 3=verbose, 4=deep debugging
DEBUG_LEVEL    = 4  # 0=off, 1=basic, 2=detailed, 3=verbose, 4=deep debugging

# Entry toggles
longEnable     = True
shortEnable    = True

# Signal configuration
useMACD        = True
alwaysEnter    = False

# Risk & sizing
highProbSLPct  = 0.1   # 5%
lowProbSLPct   = 0.05   # 2%
highProbSize   = 0.050  # 5% equity
lowProbSize    = 0.025  # 2.5% equity

# --- Position-size selection mode ---
SIZE_MODE      = 'auto'  # 'auto' | 'high' | 'low'

# Partial & TP Configuration
PARTIAL_LEVELS = [
    
    {'move_pct': 0.02, 'exit_fraction': 0.02},
    {'move_pct': 0.05, 'exit_fraction': 0.1},
    {'move_pct': 0.10, 'exit_fraction': 0.1},

]
tp_pct = 0.10  # allow HA exit only after this profit from entry is reached

# ATR-based configuration - raise min ATR threshold to ignore a flat day
# --- ATR premarket inclusion toggle ---
ATR_INCLUDE_PREMARKET = True
enableATREntry = True
enableATRStop  = False
atrPeriod      = 14
atrEntryThresh = 0.003  # 0.3 % of price

# --- VWAP filter (optional) ---
ENABLE_VWAP_FILTER     = True   # True => Long only above VWAP; Short only below VWAP
VWAP_INCLUDE_PREMARKET = True    # True => compute VWAP from 07:00; False => RTH-only (>=09:30)
VWAP_SOURCE            = 'typical'  # 'typical' (HLCC/3) or 'close'

# --- VWAP refinements (optional) ---
ENABLE_VWAP_DISTANCE_FLOOR = False      # when True, require price to be away from VWAP
VWAP_MIN_DIST_PCT          = 0.10 / 100 # 0.10% minimum distance

ENABLE_VWAP_SLOPE_FILTER   = False      # when True, require VWAP slope to agree with trade side
VWAP_SLOPE_LOOKBACK_MIN    = 3          # minutes to look back
VWAP_SLOPE_MIN_ABS_PCT     = 0.00 / 100 # optional min absolute slope (0 = any positive/negative is ok)

# --- HA vs Timeout precedence ---
HA_OVERRIDES_TIMEOUT       = True      # True => if HA is still in-favor after TP-gate, suppress timeout
HA_OVERRIDE_REQUIRE_TP     = True       # only suppress if tp_reached_* is True (your 10% gate)

# First-bar exit block (post-entry safety)
enableFirstBarExitBlock    = True
firstBarExitBlockATRThresh = 0.1
#This block does not do shit, fix it.!
# Entry-bar stop padding (first bar after entry only)
enableEntryBarStopPad = True
entryBarStopPad       = 0.02

# Timeout exit (IN MINUTES)
enableTimeout  = True
exitTimeout    = 30

# Heiken-Ashi exit
enableHAExit   = True
# >>> New toggle: '1m' or '5m'
HA_TIMEFRAME   = '5m'   # '1m' | '5m'
# === MACD timeframe toggle ===
MACD_TIMEFRAME = '1m'   # '1m' or '5m'

# MACD params
macdFast       = 12
macdSlow       = 26
macdSignal     = 9

# Two-tier MACD entry controls
recentCrossLookback   = 10
allowLateTrendEntries = True
lateTrendATRMultiplier = 1.25
lateTrendSize         = lowProbSize
lateTrendSlPct        = lowProbSLPct
# =============== v13 IMPROVEMENTS (toggle all at once) ===============

# =============== REPORT TOGGLES (print sections) ===============
SHOW_PERFORMANCE_SUMMARY = True
SHOW_RESULT_TABLES       = True     # master switch for extended tables
SHOW_DIRECTION_SUMMARY   = True
SHOW_EXIT_REASON_OUTCOME = True
SHOW_STOPLOSS_TRADES     = True     # full list of SL exits (with timestamp)
SHOW_STOPLOSS_UNIQUE     = True     # unique ticker × date for SL exits
SHOW_STOPLOSS_OFFENDERS  = True     # counts by ticker for SL exits
# ==============================================================

# =============== P2-LOCK (opposite direction after Partial 2) ===============
# Enable a rule that, once one direction hits Partial 2, blocks future entries
# on the opposite direction for the same ticker-session.
ENABLE_P2_OPPOSITE_LOCK = True

# Which direction(s) to block when the other side hits P2:
#   'BOTH'                = block opposite entries in both directions
#   'LONG_AFTER_SHORT'    = when SHORT reaches P2, block LONG entries
#   'SHORT_AFTER_LONG'    = when LONG reaches P2, block SHORT entries
P2_LOCK_DIRECTION = 'BOTH'

# Time limit for the block (minutes). None = rest of session.
P2_LOCK_MINUTES = 15
# ===========================================================================

ENABLE_V13_IMPROVEMENTS = True  # when False, behavior matches v12

# --- Early-window MACD strictness ---
# Require Tier A only for the first X minutes after 09:30 (disallow late-trend Tier B entries)
V13_REQUIRE_TIER_A_FIRST_MIN = 3 if ENABLE_V13_IMPROVEMENTS else 0

# Tier-aware timeout (give Tier A more time than Tier B)
ENABLE_TIER_AWARE_TIMEOUT = True   # flip to True to enable
TIMEOUT_TIER_A_MIN = 30.0
TIMEOUT_TIER_B_MIN = 25.0
# --- BE timing tweaks ---
# Wait until at least this many partials have filled before activating BE; v12 behavior was 1
V13_BE_AFTER_PARTIALS = 2 if ENABLE_V13_IMPROVEMENTS else 1
# Also require a minimum number of minutes elapsed since entry before BE is allowed
V13_BE_MIN_MINUTES = 2.0 if ENABLE_V13_IMPROVEMENTS else 0.0

# --- Entry initial stop widening (to survive open chop) ---
# For the first N minutes AFTER ENTRY, widen the effective stop by extra ATRs
V13_ENTRY_WIDE_MINUTES = 2.0 if ENABLE_V13_IMPROVEMENTS else 0.0
V13_ENTRY_WIDE_ATR_MULT = 1.0  if ENABLE_V13_IMPROVEMENTS else 0.0  # extra ATRs beyond the base stop

# --- Tier-B specific stop widening for percent-based stops ---
# Multiply the percent stop for Tier B (late trend). 1.0 = no change. v12 used lowProbSLPct directly.
V13_TIER_B_SL_MULT = 1.5 if ENABLE_V13_IMPROVEMENTS else 1.0

# --- ATR-stop multipliers per tier (used only if enableATRStop=True and ATR available) ---
# When improvements disabled, v12 defaults apply inside the code (1.5 or 1.0 depending on SIZE_MODE)
STOP_ATR_MULT_A = 1.2  # tighter for Tier A
STOP_ATR_MULT_B = 1.6  # wider for Tier B
# =====================================================================


# CASH / CONCURRENCY CONTROLS
USE_DYNAMIC_CASH = True
MIN_TRADE_AMOUNT = 1_000.0
ALLOW_CONCURRENT_POSITIONS = True

# ENTRY FILTERS & BEHAVIOR TOGGLES
enableORBBias = False
ORB_WINDOW_MIN = 1          # minutes
ALLOW_OPPOSITE_SAME_BAR = True
ENABLE_MAE_MFE = True

# Trade limits
limitOnePerDirection = True

# REQUEST RATE DEBUG
REQUEST_DEBUG = True
request_times = deque(maxlen=1000)

# RATE LIMIT
RESPECT_RATE_LIMIT = True
MAX_CALLS_PER_MIN = 5
RATE_LIMIT_BUFFER_SEC = 0.25

# STATE & LOGGING
trades = []
trade_logs = []
current_balance = EQUITY
available_cash = EQUITY
open_positions = 0
tz_et = pytz.timezone(TIMEZONE)

# --- Ratcheting stop helpers (module scope) ---
def _apply_step_ratchet_long(entry_price, h, cur_stop, be_set, idx):
    if RATCHET_REQUIRE_BE and not be_set:
        return cur_stop, idx
    move = (h - entry_price) / entry_price if entry_price else 0.0
    new_stop, new_idx = cur_stop, idx
    while ENABLE_RATCHET_STOPS and new_idx < len(STEP_RATCHET_LEVELS_LONG):
        trig = STEP_RATCHET_LEVELS_LONG[new_idx]["trigger_move_pct"]
        if move >= trig:
            lock = STEP_RATCHET_LEVELS_LONG[new_idx]["lock_from_entry_pct"]
            cand = entry_price * (1.0 + lock)
            new_stop = cand if new_stop is None else max(float(new_stop), float(cand))
            new_idx += 1
        else:
            break
    return new_stop, new_idx

def _apply_step_ratchet_short(entry_price, l, cur_stop, be_set, idx):
    if RATCHET_REQUIRE_BE and not be_set:
        return cur_stop, idx
    move = (entry_price - l) / entry_price if entry_price else 0.0
    new_stop, new_idx = cur_stop, idx
    while ENABLE_RATCHET_STOPS and new_idx < len(STEP_RATCHET_LEVELS_SHORT):
        trig = STEP_RATCHET_LEVELS_SHORT[new_idx]["trigger_move_pct"]
        if move >= trig:
            lock = STEP_RATCHET_LEVELS_SHORT[new_idx]["lock_from_entry_pct"]
            cand = entry_price * (1.0 - lock)
            new_stop = cand if new_stop is None else min(float(new_stop), float(cand))
            new_idx += 1
        else:
            break
    return new_stop, new_idx

def log_print(message, level=1):
    if LOG_LEVEL >= level:
        print(f"[LOG L{level}] {message}")

def debug_print(message, level=1, ticker=None):
    if DEBUG_LEVEL >= level:
        prefix = f"[DEBUG L{level}{' - ' + str(ticker) if ticker else ''}]"
        print(f"{prefix} {message}")

def log_trade_event(ticker, event, direction=None, price=None, shares=None, reason=None,
                    timestamp=None, balance_change=None, pnl_pct=None, trade_amount=None,
                    trade_id=None, stop_at_event=None):
    if trade_id is None:
        trade_id = trades[-1]['trade_id'] if trades else str(uuid.uuid4())
    trade_logs.append({
        'trade_id': trade_id,
        'timestamp': timestamp,
        'ticker': ticker,
        'event': event,
        'direction': direction,
        'price': price,
        'shares': shares,
        'reason': reason,
        'balance_change': balance_change,
        'pnl_pct': pnl_pct,
        'trade_amount': trade_amount,
        'stop': stop_at_event
    })
    # Convert timestamp to ET for log
    ts_et = None
    if timestamp is not None:
        if hasattr(timestamp, 'tz_convert'):
            ts_et = timestamp.tz_convert('US/Eastern')
        elif isinstance(timestamp, str):
            try:
                ts_obj = pd.Timestamp(timestamp)
                ts_et = ts_obj.tz_convert('US/Eastern') if ts_obj.tzinfo else ts_obj.tz_localize('UTC').tz_convert('US/Eastern')
            except Exception:
                ts_et = timestamp
        else:
            ts_et = timestamp
    else:
        ts_et = timestamp
    log_print(f"[{ts_et}] TRADE EVENT | {ticker} | {event} | "
              f"{'Direction: ' + direction + ' | ' if direction else ''}"
              f"{'Price: ' + f'{price:.4f}' + ' | ' if price is not None else ''}"
              f"{'Shares: ' + f'{shares:.4f}' + ' | ' if shares is not None else ''}"
              f"{'Reason: ' + reason if reason else ''}"
              f"{'Balance Change: ' + f'{balance_change:.2f}' if balance_change is not None else ''}"
              f"{'P&L %: ' + f'{pnl_pct:.2f}%' if pnl_pct is not None else ''}", 1)

def parse_csv(path):
    ticker_dates = []
    try:
        with open(path, 'r', newline='', encoding='utf-8-sig') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if not row or len(row) == 0 or row[0].strip().startswith('#'):
                    continue
                if len(row) >= 2:
                    ticker = ''.join(c for c in row[0].strip() if c.isprintable()).replace('\ufeff', '')
                    date_str = ''.join(c for c in row[1].strip() if c.isprintable())
                    try:
                        if '/' in date_str:
                            parts = date_str.split('/')
                            if len(parts) == 3:
                                month, day, year = parts
                                if len(year) == 2:
                                    year = f"20{year}" if int(year) <= 50 else f"19{year}"
                                iso_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                                ticker_dates.append((ticker, iso_date))
                    except ValueError as e:
                        log_print(f"Could not parse date '{date_str}': {e}", 1)
    except FileNotFoundError:
        log_print(f"CSV file not found: {path}", 1)
    except Exception as e:
        log_print(f"Error reading CSV file: {e}", 1)
    return ticker_dates

# ===== Local Databento loader (1s direct) =====
def _read_db_file(base_path_no_ext):
    pq = base_path_no_ext + '.parquet'
    cs = base_path_no_ext + '.csv'
    df = None
    if os.path.exists(pq):
        df = pd.read_parquet(pq)
    elif os.path.exists(cs):
        df = pd.read_csv(cs)
    return df

def _coerce_timestamp_utc(series_or_values):
    return pd.to_datetime(series_or_values, utc=True, errors='coerce')

def load_databento_local_1s(ticker, date_iso):
    base = f"{ticker}_{date_iso}_{DB_DATASET}_{DB_SCHEMA}"
    base_path = os.path.join(DATA_DB_DIR, base)
    df = _read_db_file(base_path)
    if df is None or df.empty:
        log_print(f"{ticker} {date_iso}: local file not found or empty at {base_path}.(parquet/csv)", 1)
        return pd.DataFrame()

    if 'timestamp' in df.columns:
        df = df.copy()
        df['timestamp'] = _coerce_timestamp_utc(df['timestamp'])
    else:
        for alt in ('ts_event','ts','time','date_time','datetime'):
            if alt in df.columns:
                df = df.copy()
                df['timestamp'] = _coerce_timestamp_utc(df[alt])
                break
    if 'timestamp' not in df.columns:
        log_print(f"{ticker} {date_iso}: no usable timestamp column in local file.", 1)
        return pd.DataFrame()

    keep = ['timestamp','open','high','low','close','volume']
    for k in keep:
        if k not in df.columns:
            if k == 'volume':
                df['volume'] = 0.0
            else:
                log_print(f"{ticker} {date_iso}: missing column '{k}' in local file.", 1)
                return pd.DataFrame()

    df = df.dropna(subset=['timestamp']).sort_values('timestamp')
    df = df.set_index(pd.DatetimeIndex(df['timestamp'], tz='UTC'))
    df = df[['open','high','low','close','volume']]
    return df

def fetch_market_data_polygon(ticker, date):
    now = time.time()
    while request_times and (now - request_times[0]) > 60:
        request_times.popleft()
    if REQUEST_DEBUG:
        debug_print(f"API rolling-count last 60s = {len(request_times)}", 2, ticker=None)

    if RESPECT_RATE_LIMIT and len(request_times) >= MAX_CALLS_PER_MIN:
        oldest = request_times[0]
        wait_until = oldest + 60 + RATE_LIMIT_BUFFER_SEC
        sleep_s = max(0.0, wait_until - now)
        if sleep_s > 0:
            log_print(f"Rate limit guard: {len(request_times)} calls in last 60s — sleeping {sleep_s:.2f}s", 1)
            time.sleep(sleep_s)
        now = time.time()
        while request_times and (now - request_times[0]) > 60:
            request_times.popleft()
        if REQUEST_DEBUG:
            debug_print(f"Post-sleep rolling-count last 60s = {len(request_times)}", 2, ticker=None)

    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{INTERVAL_MULTIPLIER}/{INTERVAL_TIMESPAN}/{date}/{date}"
    params = {'adjusted': 'true', 'sort': 'asc', 'limit': 50000, 'apiKey': API_KEY}
    try:
        send_time = time.time()
        response = requests.get(url, params)
        request_times.append(send_time)
        if response.status_code in [401, 403]:
            log_print(f"{response.status_code} Error for {ticker}: Check API key permissions", 1)
            return pd.DataFrame()
        elif response.status_code == 429:
            log_print(f"429 Rate Limited", 1)
            return pd.DataFrame()
        response.raise_for_status()
        data = response.json()
        results = data.get('results', [])
        if not results:
            log_print(f"{ticker}: API returned no results for {date}", 1)
            return pd.DataFrame()
        df = pd.DataFrame(results)
        df['t'] = pd.to_datetime(df['t'], unit='ms', utc=True)
        return df.rename(columns={'t':'timestamp','o':'open','h':'high','l':'low','c':'close','v':'volume'}).set_index('timestamp')
    except Exception as e:
        log_print(f"Error fetching data for {ticker}: {e}", 1)
        return pd.DataFrame()

def fetch_market_data(ticker, date):
    if DATA_SOURCE == 'databento_local':
        return load_databento_local_1s(ticker, date)
    else:
        return fetch_market_data_polygon(ticker, date)

def rolling_heiken(prev_open, prev_close, o, h, l, c):
    ha_close = (o + h + l + c) / 4
    ha_open  = (prev_open + prev_close) / 2
    return ha_open, ha_close

def compute_macd(series):
    if len(series) < macdSlow:
        z = pd.Series([0]*len(series))
        return z, z.copy(), z.copy()
    fast = series.ewm(span=macdFast, adjust=False).mean()
    slow = series.ewm(span=macdSlow, adjust=False).mean()
    macd_line = fast - slow
    signal_line = macd_line.ewm(span=macdSignal, adjust=False).mean()
    hist = macd_line - signal_line
    return macd_line, signal_line, hist

def resample_ohlc(df_1s: pd.DataFrame, tf: str) -> pd.DataFrame:
    """Resample 1s to tf ('1m' or '5m') OHLCV for signal logic."""
    rule = '1min' if tf == '1m' else '5min'
    out = df_1s.resample(rule).agg({
        'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
    }).dropna(subset=['open','high','low','close'])
    return out

def compute_heiken_ashi_from_ohlc(df_tf: pd.DataFrame) -> pd.DataFrame:
    """Compute classic HA on tf OHLC series."""
    ha_open = []
    ha_close = []
    # initialize from the first raw candle
    prev_open = df_tf['open'].iloc[0]
    prev_close = (df_tf['open'].iloc[0] + df_tf['high'].iloc[0] +
                  df_tf['low'].iloc[0] + df_tf['close'].iloc[0]) / 4.0
    for idx, row in df_tf.iterrows():
        o,h,l,c = row['open'], row['high'], row['low'], row['close']
        cur_ha_close = (o + h + l + c) / 4.0
        cur_ha_open  = (prev_open + prev_close) / 2.0
        ha_open.append(cur_ha_open)
        ha_close.append(cur_ha_close)
        prev_open, prev_close = cur_ha_open, cur_ha_close
    out = pd.DataFrame({'ha_open': ha_open, 'ha_close': ha_close}, index=df_tf.index)
    return out

def tf_floor(ts: pd.Timestamp, tf: str) -> pd.Timestamp:
    return ts.floor('min') if tf == '1m' else ts.floor('5min')

def tf_delta(tf: str) -> pd.Timedelta:
    return pd.Timedelta(minutes=1) if tf == '1m' else pd.Timedelta(minutes=5)

def choose_size_and_sl(high_prob: bool):
    if SIZE_MODE == 'high':
        return highProbSize, highProbSLPct
    elif SIZE_MODE == 'low':
        return lowProbSize, lowProbSLPct
    else:  # 'auto'
        return (highProbSize, highProbSLPct) if high_prob else (lowProbSize, lowProbSLPct)

# -------------- STRATEGY CORE --------------
def run_ticker(ticker, date_str):
    # v13: helper flags

    # --- P2-lock state for this ticker/session ---
    p2_time_long = None
    p2_time_short = None

    def _p2_lock_blocks(direction, now_ts):
        if not ENABLE_P2_OPPOSITE_LOCK:
            return False
        try:
            if direction == 'LONG' and P2_LOCK_DIRECTION in ('BOTH','LONG_AFTER_SHORT'):
                if p2_time_short is not None:
                    elapsed = (now_ts - p2_time_short).total_seconds() / 60.0
                    if (P2_LOCK_MINUTES is None) or (elapsed < float(P2_LOCK_MINUTES)):
                        debug_print(f"P2-lock active: block LONG, {elapsed:.1f} min after SHORT P2", 2, ticker)
                        return True
            if direction == 'SHORT' and P2_LOCK_DIRECTION in ('BOTH','SHORT_AFTER_LONG'):
                if p2_time_long is not None:
                    elapsed = (now_ts - p2_time_long).total_seconds() / 60.0
                    if (P2_LOCK_MINUTES is None) or (elapsed < float(P2_LOCK_MINUTES)):
                        debug_print(f"P2-lock active: block SHORT, {elapsed:.1f} min after LONG P2", 2, ticker)
                        return True
        except Exception:
            pass
        return False

    global entry_dt, entry_window_end, current_balance, available_cash, open_positions
    df = fetch_market_data(ticker, date_str)
    if df.empty:
        log_print(f"No data for {ticker}", 1)
        return

    # Minute-open fills: map minute->open from 1s stream
    minute_open_map = df['open'].groupby(df.index.floor('min')).first().to_dict()

    # Build TF stream for HA exit logic (signal-only)
    df_tf = resample_ohlc(df, HA_TIMEFRAME)     # '1m' or '5m'
    ha_tf = compute_heiken_ashi_from_ohlc(df_tf)
    

    # --- 1-minute ATR series for the entry filter (premarket-aware) ---
    # Define session start/end for premarket logic
    session_open = datetime.fromisoformat(f"{date_str} 09:30:00").replace(tzinfo=pytz.UTC)
    premarket_start = datetime.fromisoformat(f"{date_str} 07:00:00").replace(tzinfo=pytz.UTC)
    # Resample all data to 1min
    m1_all = df.resample('1min').agg({'open':'first','high':'max','low':'min','close':'last','volume':'sum'}).dropna()
    # Session-only bars (09:30+)
    m1_session_only = m1_all[m1_all.index >= session_open]
    # Pick ATR source
    atr_src = m1_all if ATR_INCLUDE_PREMARKET else m1_session_only
    # Compute ATR (manual, since ta-lib/ta not imported)
    tr = atr_src['high'] - atr_src['low']
    atr_series = tr.rolling(atrPeriod).mean()
    # For entry gating, at each ts, use the last completed ATR value (avoid NaN at 09:30)
    def get_atr_now(ts):
        # Use last completed ATR value before ts
        prev = atr_series.loc[:ts - pd.Timedelta(seconds=1)].dropna()
        return prev.iloc[-1] if len(prev) else np.nan
    minute_hilo = m1_all.copy()
    minute_hilo['tr_1m'] = tr
    minute_hilo['atr_1m'] = atr_series

    # --- 1-minute VWAP (premarket-aware, aligned to m1_all index) ---
    _vwap_src = m1_all if VWAP_INCLUDE_PREMARKET else m1_session_only
    if not _vwap_src.empty:
        if VWAP_SOURCE == 'typical':
            _tp = (_vwap_src['high'] + _vwap_src['low'] + _vwap_src['close']) / 3.0
        else:
            _tp = _vwap_src['close']
        _pv   = _tp * _vwap_src['volume']
        _cumv = _vwap_src['volume'].replace(0, np.nan).cumsum()
        vwap_1m = (_pv.cumsum() / _cumv).reindex(m1_all.index).ffill()
    else:
        vwap_1m = pd.Series(index=m1_all.index, dtype=float)
    # (minute_hilo is used elsewhere for debug)

    
    # --- MACD pre-compute on the chosen timeframe ---
    df_macd_tf = resample_ohlc(df, MACD_TIMEFRAME)
    _, _, macd_hist = compute_macd(df_macd_tf['close'])

    

    debug_print(f"Loaded {len(df)} data points for {ticker}", 1, ticker)
    try:
        idx0_et = df.index[0].tz_convert('US/Eastern') if hasattr(df.index[0], 'tz_convert') else df.index[0]
        idxN_et = df.index[-1].tz_convert('US/Eastern') if hasattr(df.index[-1], 'tz_convert') else df.index[-1]
        debug_print(f"Data range: {idx0_et} to {idxN_et} ET", 2, ticker)
    except Exception:
        pass
    entry_dt_et = entry_dt.tz_convert('US/Eastern') if hasattr(entry_dt, 'tz_convert') else entry_dt
    entry_window_end_et = entry_window_end.tz_convert('US/Eastern') if hasattr(entry_window_end, 'tz_convert') else entry_window_end
    debug_print(f"Entry window: {entry_dt_et} to {entry_window_end_et} ET", 2, ticker)

    entry_window_data = df[(df.index >= entry_dt) & (df.index <= entry_window_end)]
    debug_print(f"Data points within entry window: {len(entry_window_data)}", 2, ticker)

    if entry_window_data.empty:
        log_print(f"{ticker}: No data within entry window", 1)
        return

    # Opening Range (09:30 ET) computed on 1s data for the first ORB_WINDOW_MIN minutes
    session_open_local = datetime.fromisoformat(f"{date_str} 09:30:00")
    session_open_utc = tz_et.localize(session_open_local).astimezone(pytz.UTC)
    orb_start = session_open_utc
    orb_end = session_open_utc + timedelta(minutes=ORB_WINDOW_MIN)
    orb_ready = False
    orb_high = None
    orb_low = None
    orb_slice = df[(df.index >= orb_start) & (df.index < orb_end)]
    if not orb_slice.empty:
        orb_high = float(orb_slice['high'].max())
        orb_low = float(orb_slice['low'].min())
        orb_ready = True

    # Heiken-Ashi state on 1s (for first-bar padding/block). Exits use tf HA.
    ha_open, ha_close = df['open'].iloc[0], (df['open'].iloc[0]+df['close'].iloc[0])/2
    ha_buf = deque(maxlen=5)
    price_buf = []  # for MACD
    atr_buf = deque(maxlen=atrPeriod)

    in_long = in_short = False
    bar_count_long = bar_count_short = 0
    entry_price_long = entry_price_short = None
    entry_time_long = entry_time_short = None
    stop_long = stop_short = None
    remaining_shares_long = remaining_shares_short = 0
    next_partial_index_long = next_partial_index_short = 0
    long_taken = False
    short_taken = False

    schedule_exit_block_long = False
    exit_block_active_long = False
    schedule_exit_block_short = False
    exit_block_active_short = False

    current_long_idx = None
    current_long_id  = None
    current_short_idx = None
    current_short_id  = None

    entry_attempts = 0
    entry_window_active = True

    tp_reached_long = False
    tp_reached_short = False
    # --- Ratcheting stop state ---
    ratchet_index_long = 0
    ratchet_index_short = 0
    # v13 flags
    be_set_long = False
    be_set_short = False
    # Track last checked closed TF bucket
    last_checked_bucket = None

    # v13 flags
    be_set_long = False
    be_set_short = False

    # Track last checked closed TF bucket
    last_checked_bucket = None
    tf_len = tf_delta(HA_TIMEFRAME)

    for i, (ts, row) in enumerate(df.iterrows()):
        o,h,l,c = row.open, row.high, row.low, row.close

        # mark all open positions with latest price
        for t in trades:
            if t['ticker'] == ticker and t.get('remaining_shares', 0) > 0:
                t['mark_price'] = c

        # 1s HA (not used for exit, just carry state for first-bar stuff)
        ha_open, ha_close = rolling_heiken(ha_open, ha_close, o,h,l,c)
        ha_buf.append((ha_open,ha_close))
        price_buf.append(c)

        # ATR on 1s bars
        tr = max(h-l, abs(h-price_buf[-2]) if len(price_buf)>1 else 0, abs(l-price_buf[-2]) if len(price_buf)>1 else 0)
        atr = None
        if len(atr_buf) == atrPeriod:
            atr_buf.popleft()
        atr_buf.append(tr)
        if len(atr_buf) == atrPeriod:
            atr = float(np.mean(atr_buf))

        if ts > entry_window_end and entry_window_active:
            entry_window_active = False
            debug_print(f"Entry window closed at {ts.tz_convert('US/Eastern') if hasattr(ts, 'tz_convert') else ts} ET.", 2, ticker)
            if not in_long and not in_short:
                log_print(f"{ticker}: Entry window expired. No positions taken.", 1)

        # ORB finalize if not ready and window has elapsed
        if enableORBBias and not orb_ready and ts >= orb_end:
            orb_slice = df[(df.index >= orb_start) & (df.index < orb_end)]
            if not orb_slice.empty:
                orb_high = float(orb_slice['high'].max())
                orb_low = float(orb_slice['low'].min())
                orb_ready = True

        # ==== Evaluate TF-HA reversal on CLOSED TF bucket ====
        # The bucket that just CLOSED is (current_bucket - tf_len)
        current_bucket = tf_floor(ts, HA_TIMEFRAME)
        closed_bucket = current_bucket - tf_len
        if enableHAExit and last_checked_bucket != closed_bucket and closed_bucket in df_tf.index:
            pos = df_tf.index.get_loc(closed_bucket)
            if isinstance(pos, slice):  # safety
                pos = df_tf.index.get_indexer([closed_bucket])[0]
            if pos >= 1:
                prev_bucket = df_tf.index[pos-1]
                # Use precomputed HA on tf
                ha_prev_open  = ha_tf.loc[prev_bucket, 'ha_open']
                ha_prev_close = ha_tf.loc[prev_bucket, 'ha_close']
                ha_curr_open  = ha_tf.loc[closed_bucket, 'ha_open']
                ha_curr_close = ha_tf.loc[closed_bucket, 'ha_close']

                # LONG: bullish -> bearish reversal
                if in_long and tp_reached_long:
                    if (ha_prev_close > ha_prev_open) and (ha_curr_close < ha_curr_open):
                        exit_price = df_tf.loc[closed_bucket, 'close']  # exit at TF close (v7-style)
                        proceeds = remaining_shares_long * exit_price
                        if USE_DYNAMIC_CASH:
                            available_cash += proceeds
                        profit = remaining_shares_long * (exit_price - entry_price_long)
                        current_balance += profit

                        if ENABLE_MAE_MFE and current_long_idx is not None:
                            eh = trades[current_long_idx].get('extreme_high', entry_price_long)
                            el = trades[current_long_idx].get('extreme_low', entry_price_long)
                            trades[current_long_idx]['mfe_pct'] = (eh - entry_price_long) / entry_price_long * 100.0
                            trades[current_long_idx]['mae_pct'] = (el - entry_price_long) / entry_price_long * 100.0

                        reason_text = f"Heiken Ashi Reversal ({HA_TIMEFRAME})"
                        log_trade_event(ticker, 'EXIT', direction='LONG', price=exit_price,
                                        shares=remaining_shares_long, reason=reason_text,
                                        timestamp=closed_bucket, balance_change=profit,
                                        pnl_pct=(exit_price - entry_price_long)/entry_price_long * 100,
                                        stop_at_event=stop_long, trade_id=current_long_id)
                        in_long = False
                        open_positions = max(0, open_positions - 1)
                        trades[current_long_idx].update({'exit_time': closed_bucket, 'exit': exit_price,
                                                         'reason': reason_text, 'balance': current_balance,
                                                         'remaining_shares': 0, 'mark_price': exit_price})
                # SHORT: bearish -> bullish reversal
                if in_short and tp_reached_short:
                    if (ha_prev_close < ha_prev_open) and (ha_curr_close > ha_curr_open):
                        exit_price = df_tf.loc[closed_bucket, 'close']
                        cover_cost = remaining_shares_short * exit_price
                        if USE_DYNAMIC_CASH:
                            available_cash -= cover_cost
                        profit = remaining_shares_short * (entry_price_short - exit_price)
                        current_balance += profit

                        if ENABLE_MAE_MFE and current_short_idx is not None:
                            eh = trades[current_short_idx].get('extreme_high', entry_price_short)
                            el = trades[current_short_idx].get('extreme_low', entry_price_short)
                            trades[current_short_idx]['mfe_pct'] = (entry_price_short - el) / entry_price_short * 100.0
                            trades[current_short_idx]['mae_pct'] = (entry_price_short - eh) / entry_price_short * 100.0

                        reason_text = f"Heiken Ashi Reversal ({HA_TIMEFRAME})"
                        log_trade_event(ticker, 'EXIT', direction='SHORT', price=exit_price,
                                        shares=remaining_shares_short, reason=reason_text,
                                        timestamp=closed_bucket, balance_change=profit,
                                        pnl_pct=(entry_price_short - exit_price)/entry_price_short * 100,
                                        stop_at_event=stop_short, trade_id=current_short_id)
                        in_short = False
                        open_positions = max(0, open_positions - 1)
                        trades[current_short_idx].update({'exit_time': closed_bucket, 'exit': exit_price,
                                                          'reason': reason_text, 'balance': current_balance,
                                                          'remaining_shares': 0, 'mark_price': exit_price})
            last_checked_bucket = closed_bucket

        # ===== ENTRY LOGIC =====
        entered_long_this_bar = False
        entered_short_this_bar = False

        if entry_window_active and ts >= entry_dt:
            entry_attempts += 1
            if DEBUG_LEVEL >= 2:
                # Print ENTRY CHECK time in ET
                ts_et = ts.tz_convert('US/Eastern') if hasattr(ts, 'tz_convert') else ts
                debug_print(f"===================== ENTRY CHECK #{entry_attempts} at {ts_et} ET =====================", 2, ticker)
            debug_print(f"Price: {c:.2f}, Buffer size: {len(price_buf)}", 3, ticker)
            debug_print(f"MACD enabled: {useMACD}, AlwaysEnter: {alwaysEnter}", 3, ticker)
            # --- MACD debug output (show values as in user request) ---
            if useMACD and not alwaysEnter:
                closed_bucket = tf_floor(ts, MACD_TIMEFRAME) - tf_delta(MACD_TIMEFRAME)
                if closed_bucket in macd_hist.index:
                    # How many MACD bars are available up to the closed bucket?
                    bars_ready = macd_hist.loc[:closed_bucket].shape[0]
                    if DEBUG_LEVEL >= 4:
                        debug_print(f"MACD {MACD_TIMEFRAME} bars ready: {bars_ready}/{macdSlow}", 4, ticker)
                    # Recompute MACD lines for this bucket for full info
                    macd_line, signal_line, hist = compute_macd(df_macd_tf['close'])
                    macd_idx = macd_line.index.get_loc(closed_bucket)
                    # Defensive: get last 6 rows up to closed_bucket
                    last6_idx = macd_line.index[:macd_idx+1][-6:]
                    last6 = pd.DataFrame({
                        'macd': macd_line.loc[last6_idx].values,
                        'signal': signal_line.loc[last6_idx].values,
                        'hist': hist.loc[last6_idx].values
                    }, index=last6_idx)
                    debug_print(f"MACD line[-1]={macd_line.iloc[macd_idx]:.4f}, signal[-1]={signal_line.iloc[macd_idx]:.4f}, hist[-1]={hist.iloc[macd_idx]:.4f}", 4, ticker)
                    debug_print("Last 6 MACD rows:\n  macd  signal    hist", 4, ticker)
                    for i in range(len(last6)):
                        debug_print(f"{last6['macd'].iloc[i]:.4f}  {last6['signal'].iloc[i]:.4f}  {last6['hist'].iloc[i]:.4f}", 4, ticker)
                else:
                    if DEBUG_LEVEL >= 4:
                        debug_print(f"MACD {MACD_TIMEFRAME} not ready: missing closed bucket {closed_bucket}", 4, ticker)

            # --- ATR debug output ---
            minute_bucket = ts.floor('min')
            abr_current = minute_hilo.at[minute_bucket, 'atr_1m'] if minute_bucket in minute_hilo.index else None
            # --- ATR debug: not ready ---
            if DEBUG_LEVEL >= 4 and enableATREntry and abr_current is None:
                debug_print("ATR-1m not ready: need 14 closed 1m bars", 4, ticker)
            # Always show the current ATR vs threshold at L4
            if DEBUG_LEVEL >= 4 and enableATREntry:
                abr_str = f"{abr_current:.6f}" if abr_current is not None else "N/A"
                debug_print(f"ATR Entry Threshold: {atrEntryThresh:.6f}, Current ATR: {abr_str}", 4, ticker)

            if not ALLOW_CONCURRENT_POSITIONS and open_positions > 0:
                log_trade_event(ticker, 'ENTRY_SKIPPED', direction=None, price=None, shares=None,
                                 reason='CONCURRENCY_LIMIT', timestamp=ts)
            else:
                # ----- MACD tier logic using MACD_TIMEFRAME -----
                atr_valid_long = atr_valid_short = True
                tier_long = tier_short = None
                if useMACD and not alwaysEnter:
                    closed_bucket = tf_floor(ts, MACD_TIMEFRAME) - tf_delta(MACD_TIMEFRAME)
                    if closed_bucket in macd_hist.index:
                        recent = macd_hist.loc[:closed_bucket].tail(max(2, recentCrossLookback))
                        if len(recent) >= 2:
                            bullish_now  = recent.iloc[-1] > 0
                            bearish_now  = recent.iloc[-1] < 0
                            bullish_prev = ((recent > 0) & (recent.shift(1) < 0)).any()
                            bearish_prev = ((recent < 0) & (recent.shift(1) > 0)).any()
                            if DEBUG_LEVEL >= 4:
                                debug_print(f"hist_now={recent.iloc[-1]:.4f}  need >0 for LONG / <0 for SHORT", 4, ticker)
                                debug_print(f"recent_cross_up={((recent>0)&(recent.shift(1)<0)).any()}  "
                                            f"recent_cross_down={((recent<0)&(recent.shift(1)>0)).any()}", 4, ticker)
                            if bullish_now and bullish_prev:
                                tier_long = 'A'
                            elif bullish_now and allowLateTrendEntries:
                                tier_long = 'B'
                            if bearish_now and bearish_prev:
                                tier_short = 'A'
                            elif bearish_now and allowLateTrendEntries:
                                tier_short = 'B'
                elif alwaysEnter:
                    tier_long = tier_short = 'A'
                    atr_valid_long = atr_valid_short = True
                minute_bucket = ts.floor('min')
                abr_current = minute_hilo.at[minute_bucket, 'atr_1m'] if minute_bucket in minute_hilo.index else None

                # VWAP at this minute (use last known VWAP up to minute_bucket)
                vwap_now = None
                vwap_prev = None
                if 'vwap_1m' in locals():
                    try:
                        v_slice = vwap_1m.loc[:minute_bucket].dropna()
                        if len(v_slice):
                            vwap_now = float(v_slice.iloc[-1])
                            if ENABLE_VWAP_SLOPE_FILTER and len(v_slice) > VWAP_SLOPE_LOOKBACK_MIN:
                                vwap_prev = float(v_slice.iloc[-1 - VWAP_SLOPE_LOOKBACK_MIN])
                    except Exception:
                        pass

                def _vwap_slope_ok(side: str) -> bool:
                    if not ENABLE_VWAP_SLOPE_FILTER:
                        return True
                    if vwap_now is None or vwap_prev is None:
                        return False  # not ready => block (and we’ll log why)
                    dv = vwap_now - vwap_prev
                    if vwap_prev != 0:
                        dv_pct = dv / vwap_prev
                    else:
                        dv_pct = 0.0
                    if side == 'LONG':
                        return (dv > 0) and (abs(dv_pct) >= VWAP_SLOPE_MIN_ABS_PCT)
                    else:  # SHORT
                        return (dv < 0) and (abs(dv_pct) >= VWAP_SLOPE_MIN_ABS_PCT)

                def _vwap_distance_ok(side: str) -> bool:
                    if not ENABLE_VWAP_DISTANCE_FLOOR:
                        return True
                    if vwap_now is None:
                        return False
                    if vwap_now == 0:
                        return False
                    dist_pct = (c - vwap_now) / vwap_now
                    if side == 'LONG':
                        return dist_pct >= VWAP_MIN_DIST_PCT
                    else:  # SHORT
                        return (-dist_pct) >= VWAP_MIN_DIST_PCT
                # v13: log both tier assignments and recent-cross state
                if DEBUG_LEVEL >= 3:
                    _hist_now = recent.iloc[-1] if 'recent' in locals() and len(recent) else float('nan')
                    _cross_up = ((recent > 0) & (recent.shift(1) < 0)).any() if 'recent' in locals() and len(recent) >= 2 else False
                    _cross_dn = ((recent < 0) & (recent.shift(1) > 0)).any() if 'recent' in locals() and len(recent) >= 2 else False
                    debug_print(f"MACD tiers → Long={tier_long or 'None'}  Short={tier_short or 'None'}  hist_now={_hist_now:.4f}  cross_up={_cross_up}  cross_down={_cross_dn}", 3, ticker)

                # v13: first-X-minutes require Tier A only (disallow B) after session open
                if ENABLE_V13_IMPROVEMENTS and V13_REQUIRE_TIER_A_FIRST_MIN > 0:
                    _mso = (ts - session_open_utc).total_seconds() / 60.0
                    if _mso < V13_REQUIRE_TIER_A_FIRST_MIN:
                        if tier_long == 'B': tier_long = None
                        if tier_short == 'B': tier_short = None

                # --- ATR debug: not ready ---
                if DEBUG_LEVEL >= 4 and enableATREntry and abr_current is None:
                    debug_print("ATR-1m not ready: need 14 closed 1m bars", 4, ticker)
                # Always show the current ATR vs threshold at L4
                if DEBUG_LEVEL >= 4 and enableATREntry:
                    abr_str = f"{abr_current:.6f}" if abr_current is not None else "N/A"
                    debug_print(f"ATR Entry Threshold: {atrEntryThresh:.6f}, Current ATR: {abr_str}", 4, ticker)
                # Gate entries by 1m ATR if enabled
                if enableATREntry:
                    atr_valid_long  = (abr_current is not None) and (abr_current > atrEntryThresh)
                    atr_valid_short = (abr_current is not None) and (abr_current > atrEntryThresh)
                else:
                    atr_valid_long = atr_valid_short = True

                minute_open = minute_open_map.get(minute_bucket, o)  # fallback

                # --- LONG ENTRY ---
                can_long = (
                    (tier_long is not None)
                    and longEnable
                    and atr_valid_long
                    and (ALLOW_OPPOSITE_SAME_BAR or not entered_short_this_bar)
                    and (not _p2_lock_blocks('LONG', ts))
                    and (not limitOnePerDirection or not long_taken)
                )
                # VWAP filter for LONG
                if ENABLE_VWAP_FILTER:
                    can_long = can_long and (vwap_now is not None and c >= vwap_now)
                can_long = can_long and _vwap_distance_ok('LONG')
                can_long = can_long and _vwap_slope_ok('LONG')
                # ── WHY NO LONG?  (debug only) ────────────────────────────────
                if DEBUG_LEVEL >= 3 and not can_long:
                    reasons = []
                    if tier_long is None:                     reasons.append("MACD tier = None")
                    if not longEnable:                        reasons.append("longEnable = False")
                    if not atr_valid_long:                    reasons.append(f"ATR {abr_current or 'N/A'} ≤ {atrEntryThresh}")
                    if not (ALLOW_OPPOSITE_SAME_BAR or not entered_short_this_bar):
                                                      reasons.append("Opposite side already entered this bar")
                    if limitOnePerDirection and long_taken:   reasons.append("Limit-One-Per-Dir: long already taken")
                    if ENABLE_VWAP_FILTER:
                        if vwap_now is None: reasons.append("VWAP not ready")
                        elif c < vwap_now:   reasons.append(f"VWAP: price {c:.2f} < {vwap_now:.2f}")
                    if ENABLE_VWAP_DISTANCE_FLOOR and vwap_now is not None:
                        dist_pct = (c - vwap_now)/vwap_now
                        if dist_pct < VWAP_MIN_DIST_PCT:
                            reasons.append(f"VWAP dist {dist_pct*100:.2f}% < {VWAP_MIN_DIST_PCT*100:.2f}%")
                    if ENABLE_VWAP_SLOPE_FILTER:
                        if vwap_now is None or vwap_prev is None:
                            reasons.append("VWAP slope not ready")
                        else:
                            dv_pct = (vwap_now - vwap_prev)/(vwap_prev if vwap_prev else 1.0)
                            if not _vwap_slope_ok('LONG'):
                                reasons.append(f"slope {dv_pct*100:.2f}% not > {VWAP_SLOPE_MIN_ABS_PCT*100:.2f}%")
                    if reasons or _p2_lock_blocks('LONG', ts):
                        if _p2_lock_blocks('LONG', ts): reasons.append('blocked by P2-lock')
                        debug_print("No LONG entry → " + "; ".join(reasons), 3, ticker)
                # ──────────────────────────────────────────────────────────────
                if can_long:
                    if SIZE_MODE == 'auto':
                        if tier_long == 'A':
                            size_long, sl_pct_long = highProbSize, highProbSLPct
                        else:
                            size_long, sl_pct_long = lateTrendSize, lowProbSLPct
                    elif SIZE_MODE == 'high':
                        size_long, sl_pct_long = highProbSize, highProbSLPct
                    else:
                        size_long, sl_pct_long = lowProbSize, lowProbSLPct

                    base_capital = (available_cash if USE_DYNAMIC_CASH else EQUITY)
                    trade_amount_long = base_capital * size_long

                    if USE_DYNAMIC_CASH:
                        if trade_amount_long < MIN_TRADE_AMOUNT:
                            log_trade_event(ticker, 'ENTRY_SKIPPED', direction='LONG', price=c, shares=0,
                                            reason=f'INSUFFICIENT_MIN_NOTIONAL<{MIN_TRADE_AMOUNT}', timestamp=ts)
                        elif trade_amount_long > available_cash:
                            log_trade_event(ticker, 'ENTRY_SKIPPED', direction='LONG', price=c, shares=0,
                                            reason='INSUFFICIENT_CASH', timestamp=ts)
                        else:
                            in_long = True
                            entry_price_long = float(minute_open)  # minute-open fill
                            entry_time_long  = ts
                            remaining_shares_long = trade_amount_long / entry_price_long
                            next_partial_index_long = 0
                            tp_reached_long = False

                            if enableATRStop and atr is not None:
                                if ENABLE_V13_IMPROVEMENTS:
                                    base_mult = STOP_ATR_MULT_A if tier_long == 'A' else STOP_ATR_MULT_B
                                else:
                                    base_mult = (1.5 if SIZE_MODE!='low' else 1.0)
                                stop_long = entry_price_long - base_mult * atr
                            else:
                                _slp = sl_pct_long
                                if ENABLE_V13_IMPROVEMENTS and tier_long == 'B':
                                    _slp = sl_pct_long * V13_TIER_B_SL_MULT
                                stop_long = entry_price_long * (1 - _slp)

                            trade_id = str(uuid.uuid4())
                            available_cash -= trade_amount_long
                            open_positions += 1
                            current_long_idx = len(trades)
                            current_long_id  = trade_id
                            # Tier-aware timeout logic
                            entry_tier = tier_long
                            if ENABLE_TIER_AWARE_TIMEOUT and entry_tier in ('A', 'B'):
                                this_timeout_min = TIMEOUT_TIER_A_MIN if entry_tier == 'A' else TIMEOUT_TIER_B_MIN
                            else:
                                this_timeout_min = exitTimeout
                            trades.append({
                                'trade_id': trade_id, 'ticker': ticker, 'direction': 'LONG',
                                'entry_time': ts, 'entry': entry_price_long,
                                'size': size_long,
                                'remaining_shares': remaining_shares_long, 'stop': stop_long,
                                'balance': current_balance, 'trade_amount': trade_amount_long,
                                'sl_pct': sl_pct_long, 'mark_price': c,
                                'extreme_high': c, 'extreme_low': c,
                                'mfe_pct': np.nan, 'mae_pct': np.nan, 'pnl_pct': np.nan,
                                'timeout_min': float(this_timeout_min)
                            })
                            debug_print(f"Set timeout for LONG entry (tier={entry_tier}) → {this_timeout_min} min", 2, ticker)
                            log_trade_event(ticker, 'ENTRY', direction='LONG', price=entry_price_long,
                                            shares=remaining_shares_long,
                                            timestamp=ts, trade_amount=trade_amount_long, trade_id=trade_id,
                                            stop_at_event=stop_long)
                            entered_long_this_bar = True
                            long_taken = True
                            schedule_exit_block_long = True
                            exit_block_active_long = False
                    else:
                        in_long = True
                        entry_price_long = float(minute_open)
                        entry_time_long  = ts
                        remaining_shares_long = trade_amount_long / entry_price_long
                        next_partial_index_long = 0
                        tp_reached_long = False
                        if enableATRStop and atr is not None:
                            if ENABLE_V13_IMPROVEMENTS:
                                base_mult = STOP_ATR_MULT_A if tier_long == 'A' else STOP_ATR_MULT_B
                            else:
                                base_mult = (1.5 if SIZE_MODE!='low' else 1.0)
                            stop_long = entry_price_long - base_mult * atr
                        else:
                            _slp = sl_pct_long
                            if ENABLE_V13_IMPROVEMENTS and tier_long == 'B':
                                _slp = sl_pct_long * V13_TIER_B_SL_MULT
                            stop_long = entry_price_long * (1 - _slp)
                        trade_id = str(uuid.uuid4())
                        open_positions += 1
                        current_long_idx = len(trades)
                        current_long_id  = trade_id
                        # Tier-aware timeout logic
                        entry_tier = tier_long
                        if ENABLE_TIER_AWARE_TIMEOUT and entry_tier in ('A', 'B'):
                            this_timeout_min = TIMEOUT_TIER_A_MIN if entry_tier == 'A' else TIMEOUT_TIER_B_MIN
                        else:
                            this_timeout_min = exitTimeout
                        trades.append({
                            'trade_id': trade_id, 'ticker': ticker, 'direction': 'LONG',
                            'entry_time': ts, 'entry': entry_price_long,
                            'size': size_long,
                            'remaining_shares': remaining_shares_long, 'stop': stop_long,
                            'balance': current_balance, 'trade_amount': trade_amount_long,
                            'sl_pct': sl_pct_long, 'mark_price': c,
                            'extreme_high': c, 'extreme_low': c,
                            'mfe_pct': np.nan, 'mae_pct': np.nan, 'pnl_pct': np.nan,
                            'timeout_min': float(this_timeout_min),
                            'entry_tier': entry_tier
                        })
                        debug_print(f"Set timeout for LONG entry (tier={entry_tier}) → {this_timeout_min} min", 2, ticker)
                        log_trade_event(ticker, 'ENTRY', direction='LONG', price=entry_price_long,
                                        shares=remaining_shares_long,
                                        timestamp=ts, trade_amount=trade_amount_long, trade_id=trade_id,
                                        stop_at_event=stop_long)
                        entered_long_this_bar = True
                        long_taken = True
                        schedule_exit_block_long = True
                        exit_block_active_long = False

                # --- SHORT ENTRY ---
                can_short = (
                    (tier_short is not None)
                    and shortEnable
                    and atr_valid_short
                    and (ALLOW_OPPOSITE_SAME_BAR or not entered_long_this_bar)
                    and (not _p2_lock_blocks('SHORT', ts))
                    and (not limitOnePerDirection or not short_taken)
                )
                # VWAP filter for SHORT
                if ENABLE_VWAP_FILTER:
                    can_short = can_short and (vwap_now is not None and c <= vwap_now)
                can_short = can_short and _vwap_distance_ok('SHORT')
                can_short = can_short and _vwap_slope_ok('SHORT')
                # ── WHY NO SHORT?  (debug only) ───────────────────────────────
                if DEBUG_LEVEL >= 3 and not can_short:
                    reasons = []
                    if tier_short is None:                    reasons.append("MACD tier = None")
                    if not shortEnable:                       reasons.append("shortEnable = False")
                    if not atr_valid_short:                   reasons.append(f"ATR {abr_current or 'N/A'} ≤ {atrEntryThresh}")
                    if not (ALLOW_OPPOSITE_SAME_BAR or not entered_long_this_bar):
                                                      reasons.append("Opposite side already entered this bar")
                    if limitOnePerDirection and short_taken:  reasons.append("Limit-One-Per-Dir: short already taken")
                    if ENABLE_VWAP_FILTER:
                        if vwap_now is None: reasons.append("VWAP not ready")
                        elif c > vwap_now:   reasons.append(f"VWAP: price {c:.2f} > {vwap_now:.2f}")
                    if ENABLE_VWAP_DISTANCE_FLOOR and vwap_now is not None:
                        dist_pct = (c - vwap_now)/vwap_now
                        if (-dist_pct) < VWAP_MIN_DIST_PCT:
                            reasons.append(f"VWAP dist {abs(dist_pct)*100:.2f}% < {VWAP_MIN_DIST_PCT*100:.2f}%")
                    if ENABLE_VWAP_SLOPE_FILTER:
                        if vwap_now is None or vwap_prev is None:
                            reasons.append("VWAP slope not ready")
                        else:
                            dv_pct = (vwap_now - vwap_prev)/(vwap_prev if vwap_prev else 1.0)
                            if not _vwap_slope_ok('SHORT'):
                                reasons.append(f"slope {dv_pct*100:.2f}% not < -{VWAP_SLOPE_MIN_ABS_PCT*100:.2f}%")
                    if reasons or _p2_lock_blocks('SHORT', ts):
                        if _p2_lock_blocks('SHORT', ts): reasons.append('blocked by P2-lock')
                        debug_print("No SHORT entry → " + "; ".join(reasons), 3, ticker)
                # ──────────────────────────────────────────────────────────────
                if can_short:
                    if SIZE_MODE == 'auto':
                        if tier_short == 'A':
                            size_short, sl_pct_short = highProbSize, highProbSLPct
                        else:
                            size_short, sl_pct_short = lateTrendSize, lowProbSLPct
                    elif SIZE_MODE == 'high':
                        size_short, sl_pct_short = highProbSize, highProbSLPct
                    else:
                        size_short, sl_pct_short = lowProbSize, lowProbSLPct

                    base_capital = (available_cash if USE_DYNAMIC_CASH else EQUITY)
                    trade_amount_short = base_capital * size_short

                    if USE_DYNAMIC_CASH:
                        if trade_amount_short < MIN_TRADE_AMOUNT:
                            log_trade_event(ticker, 'ENTRY_SKIPPED', direction='SHORT', price=c, shares=0,
                                            reason=f'INSUFFICIENT_MIN_NOTIONAL<{MIN_TRADE_AMOUNT}', timestamp=ts)
                        else:
                            in_short = True
                            entry_price_short = float(minute_open)  # minute-open fill
                            entry_time_short  = ts
                            remaining_shares_short = trade_amount_short / entry_price_short
                            next_partial_index_short = 0
                            tp_reached_short = False

                            if enableATRStop and atr is not None:
                                if ENABLE_V13_IMPROVEMENTS:
                                    base_mult = STOP_ATR_MULT_A if tier_short == 'A' else STOP_ATR_MULT_B
                                else:
                                    base_mult = (1.5 if SIZE_MODE!='low' else 1.0)
                                stop_short = entry_price_short + base_mult * atr
                            else:
                                _slp = sl_pct_short
                                if ENABLE_V13_IMPROVEMENTS and tier_short == 'B':
                                    _slp = sl_pct_short * V13_TIER_B_SL_MULT
                                stop_short = entry_price_short * (1 + _slp)

                            trade_id = str(uuid.uuid4())
                            available_cash += trade_amount_short
                            open_positions += 1
                            current_short_idx = len(trades)
                            current_short_id  = trade_id
                            # Tier-aware timeout logic
                            entry_tier = tier_short
                            if ENABLE_TIER_AWARE_TIMEOUT and entry_tier in ('A', 'B'):
                                this_timeout_min = TIMEOUT_TIER_A_MIN if entry_tier == 'A' else TIMEOUT_TIER_B_MIN
                            else:
                                this_timeout_min = exitTimeout
                            trades.append({
                                'trade_id': trade_id, 'ticker': ticker, 'direction': 'SHORT',
                                'entry_time': ts, 'entry': entry_price_short,
                                'size': size_short,
                                'remaining_shares': remaining_shares_short, 'stop': stop_short,
                                'balance': current_balance, 'trade_amount': trade_amount_short,
                                'sl_pct': sl_pct_short, 'mark_price': c,
                                'extreme_high': c, 'extreme_low': c,
                                'mfe_pct': np.nan, 'mae_pct': np.nan, 'pnl_pct': np.nan,
                                'timeout_min': float(this_timeout_min),
                                'entry_tier': entry_tier
                            })
                            debug_print(f"Set timeout for SHORT entry (tier={entry_tier}) → {this_timeout_min} min", 2, ticker)
                            log_trade_event(ticker, 'ENTRY', direction='SHORT', price=entry_price_short,
                                            shares=remaining_shares_short,
                                            timestamp=ts, trade_amount=trade_amount_short, trade_id=trade_id,
                                            stop_at_event=stop_short)
                            entered_short_this_bar = True
                            short_taken = True
                            schedule_exit_block_short = True
                            exit_block_active_short = False
                    else:
                        in_short = True
                        entry_price_short = float(minute_open)
                        entry_time_short  = ts
                        remaining_shares_short = trade_amount_short / entry_price_short
                        next_partial_index_short = 0
                        tp_reached_short = False
                        if enableATRStop and atr is not None:
                            if ENABLE_V13_IMPROVEMENTS:
                                base_mult = STOP_ATR_MULT_A if tier_short == 'A' else STOP_ATR_MULT_B
                            else:
                                base_mult = (1.5 if SIZE_MODE!='low' else 1.0)
                            stop_short = entry_price_short + base_mult * atr
                        else:
                            _slp = sl_pct_short
                            if ENABLE_V13_IMPROVEMENTS and tier_short == 'B':
                                _slp = sl_pct_short * V13_TIER_B_SL_MULT
                            stop_short = entry_price_short * (1 + _slp)
                        trade_id = str(uuid.uuid4())
                        open_positions += 1
                        current_short_idx = len(trades)
                        current_short_id  = trade_id
                        # Tier-aware timeout logic
                        entry_tier = tier_short
                        if ENABLE_TIER_AWARE_TIMEOUT and entry_tier in ('A', 'B'):
                            this_timeout_min = TIMEOUT_TIER_A_MIN if entry_tier == 'A' else TIMEOUT_TIER_B_MIN
                        else:
                            this_timeout_min = exitTimeout
                        trades.append({
                            'trade_id': trade_id, 'ticker': ticker, 'direction': 'SHORT',
                            'entry_time': ts, 'entry': entry_price_short,
                            'size': size_short,
                            'remaining_shares': remaining_shares_short, 'stop': stop_short,
                            'balance': current_balance, 'trade_amount': trade_amount_short,
                            'sl_pct': sl_pct_short, 'mark_price': c,
                            'extreme_high': c, 'extreme_low': c,
                            'mfe_pct': np.nan, 'mae_pct': np.nan, 'pnl_pct': np.nan,
                            'timeout_min': float(this_timeout_min),
                            'entry_tier': entry_tier
                        })
                        debug_print(f"Set timeout for SHORT entry (tier={entry_tier}) → {this_timeout_min} min", 2, ticker)
                        log_trade_event(ticker, 'ENTRY', direction='SHORT', price=entry_price_short,
                                        shares=remaining_shares_short,
                                        timestamp=ts, trade_amount=trade_amount_short, trade_id=trade_id,
                                        stop_at_event=stop_short)
                        entered_short_this_bar = True
                        short_taken = True
                        schedule_exit_block_short = True
                        exit_block_active_short = False

        # ===== LONG MANAGEMENT =====
        if in_long and current_long_idx is not None:
                bar_count_long += 1  # seconds, for first-bar padding/block

                if ENABLE_MAE_MFE:
                    trades[current_long_idx]['extreme_high'] = max(trades[current_long_idx]['extreme_high'], h)
                    trades[current_long_idx]['extreme_low']  = min(trades[current_long_idx]['extreme_low'],  l)

                effective_stop_long = stop_long
                if enableEntryBarStopPad and bar_count_long == 1:
                    effective_stop_long = stop_long - entryBarStopPad
                # v13: widen early effective stop for first N minutes after entry using extra ATRs
                if ENABLE_V13_IMPROVEMENTS and V13_ENTRY_WIDE_MINUTES > 0 and entry_time_long is not None:
                    _elapsed_min = (ts - entry_time_long).total_seconds() / 60.0
                    if _elapsed_min < V13_ENTRY_WIDE_MINUTES and atr is not None and V13_ENTRY_WIDE_ATR_MULT > 0:
                        _wider = entry_price_long - V13_ENTRY_WIDE_ATR_MULT * atr
                        effective_stop_long = min(effective_stop_long, _wider)

                if l <= effective_stop_long:
                    proceeds = remaining_shares_long * effective_stop_long
                    if USE_DYNAMIC_CASH:
                        available_cash += proceeds
                    loss = remaining_shares_long * (effective_stop_long - entry_price_long)
                    current_balance += loss

                    if ENABLE_MAE_MFE:
                        eh = trades[current_long_idx].get('extreme_high', entry_price_long)
                        el = trades[current_long_idx].get('extreme_low', entry_price_long)
                        trades[current_long_idx]['mfe_pct'] = (eh - entry_price_long) / entry_price_long * 100.0
                        trades[current_long_idx]['mae_pct'] = (el - entry_price_long) / entry_price_long * 100.0

                    stop_reason_long = 'BE Stop' if abs(float(effective_stop_long) - float(entry_price_long)) <= max(0.005, float(entry_price_long)*1e-4) else 'Stop Loss'
                    log_trade_event(ticker, 'EXIT', direction='LONG', price=effective_stop_long, shares=remaining_shares_long,
                                    reason=stop_reason_long, timestamp=ts, balance_change=loss,
                                    pnl_pct=(effective_stop_long - entry_price_long)/entry_price_long * 100,
                                    stop_at_event=effective_stop_long, trade_id=current_long_id)
                    in_long = False
                    open_positions = max(0, open_positions - 1)
                    trades[current_long_idx].update({'exit_time': ts, 'exit': effective_stop_long, 'reason': 'stop',
                                                    'balance': current_balance, 'remaining_shares': 0, 'mark_price': effective_stop_long})
                else:
                    # Partials/BE
                    while next_partial_index_long < len(PARTIAL_LEVELS):
                        move_pct   = PARTIAL_LEVELS[next_partial_index_long]['move_pct']
                        exit_frac  = PARTIAL_LEVELS[next_partial_index_long]['exit_fraction']
                        target_price = entry_price_long * (1 + move_pct)
                        if h >= target_price:
                            fill_price = target_price
                            shares_to_sell = remaining_shares_long * exit_frac
                            proceeds = shares_to_sell * fill_price
                            if USE_DYNAMIC_CASH:
                                available_cash += proceeds
                            profit = shares_to_sell * (fill_price - entry_price_long)
                            current_balance += profit
                            remaining_shares_long -= shares_to_sell
                            trades[current_long_idx]['remaining_shares'] = remaining_shares_long
                            log_trade_event(ticker, 'PARTIAL', direction='LONG', price=fill_price, shares=shares_to_sell,
                                            reason=f'Partial level {next_partial_index_long + 1}',
                                            timestamp=ts, balance_change=profit,
                                            pnl_pct=(fill_price - entry_price_long)/entry_price_long * 100,
                                            stop_at_event=stop_long, trade_id=current_long_id)
                            next_partial_index_long += 1
                            # v13: mark LONG Partial 2 time
                            if next_partial_index_long == 2 and p2_time_long is None:
                                p2_time_long = ts
                                debug_print('P2 reached (LONG) → starting opposite lock window', 2, ticker)
                            # v13: BE activation after required partials and min minutes (when enabled)
                            _elapsed_min = (ts - entry_time_long).total_seconds() / 60.0 if entry_time_long is not None else 0.0
                            _need_partials = V13_BE_AFTER_PARTIALS if ENABLE_V13_IMPROVEMENTS else 1
                            _need_minutes  = V13_BE_MIN_MINUTES if ENABLE_V13_IMPROVEMENTS else 0.0
                            if (not be_set_long) and (next_partial_index_long >= _need_partials) and (_elapsed_min >= _need_minutes):
                                stop_long = entry_price_long
                                trades[current_long_idx]['stop'] = stop_long
                                be_set_long = True
                                log_trade_event(ticker, 'BE', direction='LONG', price=stop_long, shares=remaining_shares_long,
                                                reason='Breakeven activated', timestamp=ts,
                                                pnl_pct=(stop_long - entry_price_long)/entry_price_long * 100,
                                                stop_at_event=stop_long, trade_id=current_long_id)
                            # one-partial-per-bar pacing  ← ADDED
                            if not ALLOW_MULTI_PARTIALS_PER_BAR:
                                break
                        else:
                            break
                    # --- Step ratchet (LONG) ---
                    _prev_stop = stop_long
                    stop_long, ratchet_index_long = _apply_step_ratchet_long(
                        entry_price_long, h, stop_long, be_set_long, ratchet_index_long
                    )
                    if stop_long is not None and _prev_stop is not None and stop_long > _prev_stop + 1e-6:
                        trades[current_long_idx]['stop'] = stop_long
                        log_trade_event(ticker, 'RATCHET', direction='LONG', price=stop_long, shares=remaining_shares_long,
                                        reason=f'Stop raised to {((stop_long/entry_price_long)-1)*100:.2f}%',
                                        timestamp=ts, stop_at_event=stop_long, trade_id=current_long_id)

                    # Gate for HA exit
                    if not tp_reached_long and entry_price_long is not None:
                        if h >= entry_price_long * (1 + tp_pct):
                            tp_reached_long = True

                    # Minutes-based timeout on 1s bars
                    if enableTimeout and entry_time_long is not None:
                        elapsed_min = (ts - entry_time_long).total_seconds() / 60.0
                        timeout_limit = float(trades[current_long_idx].get('timeout_min', exitTimeout))
                        entry_tier = trades[current_long_idx].get('entry_tier')
                        # --- HA vs Timeout precedence ---
                        def _ha_trend_favors(side: str, ts_check):
                            try:
                                closed_bucket = tf_floor(ts, HA_TIMEFRAME) - tf_delta(HA_TIMEFRAME)
                                if closed_bucket in ha_tf.index:
                                    ha_open = ha_tf.loc[closed_bucket, 'ha_open']
                                    ha_close = ha_tf.loc[closed_bucket, 'ha_close']
                                    ha_curr_bullish = ha_close > ha_open
                                    if side == 'LONG':
                                        return ha_curr_bullish
                                    else:
                                        return not ha_curr_bullish
                            except Exception:
                                return False
                            return False
                        suppress_timeout = False
                        if HA_OVERRIDES_TIMEOUT and enableHAExit:
                            tp_ok = (not HA_OVERRIDE_REQUIRE_TP) or bool(tp_reached_long)
                            if tp_ok and _ha_trend_favors('LONG', ts):
                                suppress_timeout = True
                        if elapsed_min > timeout_limit and not exit_block_active_long:
                            if suppress_timeout:
                                debug_print(f"Timeout {elapsed_min:.2f}m suppressed by HA trend (still favorable)", 2, ticker)
                            else:
                                timeout_reason = f"Timeout (Tier {entry_tier}, {int(round(timeout_limit))}m)" if ENABLE_TIER_AWARE_TIMEOUT and entry_tier in ('A','B') else "Timeout"
                                debug_print(f"LONG timeout exit after {elapsed_min:.2f} min (limit {timeout_limit:.2f}m)", 2, ticker)
                                proceeds = remaining_shares_long * c
                                if USE_DYNAMIC_CASH:
                                    available_cash += proceeds
                                profit = remaining_shares_long * (c - entry_price_long)
                                current_balance += profit

                                if ENABLE_MAE_MFE:
                                    eh = trades[current_long_idx].get('extreme_high', entry_price_long)
                                    el = trades[current_long_idx].get('extreme_low', entry_price_long)
                                    trades[current_long_idx]['mfe_pct'] = (eh - entry_price_long) / entry_price_long * 100.0
                                    trades[current_long_idx]['mae_pct'] = (el - entry_price_long) / entry_price_long * 100.0

                                log_trade_event(ticker, 'EXIT', direction='LONG', price=c, shares=remaining_shares_long,
                                                reason=timeout_reason, timestamp=ts, balance_change=profit,
                                                pnl_pct=(c - entry_price_long)/entry_price_long * 100,
                                                stop_at_event=stop_long, trade_id=current_long_id)
                                in_long = False
                                open_positions = max(0, open_positions - 1)
                                trades[current_long_idx].update({'exit_time': ts, 'exit': c, 'reason': 'timeout',
                                                                'balance': current_balance, 'remaining_shares': 0, 'mark_price': c})
                    exit_block_active_long = False

            # ===== SHORT MANAGEMENT =====
        if in_short and current_short_idx is not None:
            bar_count_short += 1

            if ENABLE_MAE_MFE:
                trades[current_short_idx]['extreme_high'] = max(trades[current_short_idx]['extreme_high'], h)
                trades[current_short_idx]['extreme_low']  = min(trades[current_short_idx]['extreme_low'],  l)

            effective_stop_short = stop_short
            if enableEntryBarStopPad and bar_count_short == 1:
                effective_stop_short = stop_short + entryBarStopPad
            # v13: widen early effective stop for first N minutes after entry using extra ATRs
            if ENABLE_V13_IMPROVEMENTS and V13_ENTRY_WIDE_MINUTES > 0 and entry_time_short is not None:
                _elapsed_min = (ts - entry_time_short).total_seconds() / 60.0
                if _elapsed_min < V13_ENTRY_WIDE_MINUTES and atr is not None and V13_ENTRY_WIDE_ATR_MULT > 0:
                    _wider = entry_price_short + V13_ENTRY_WIDE_ATR_MULT * atr
                    effective_stop_short = max(effective_stop_short, _wider)

            if h >= effective_stop_short:
                cover_cost = remaining_shares_short * effective_stop_short
                if USE_DYNAMIC_CASH:
                    available_cash -= cover_cost
                loss = remaining_shares_short * (entry_price_short - effective_stop_short)
                current_balance += loss

                if ENABLE_MAE_MFE:
                    eh = trades[current_short_idx].get('extreme_high', entry_price_short)
                    el = trades[current_short_idx].get('extreme_low', entry_price_short)
                    trades[current_short_idx]['mfe_pct'] = (entry_price_short - el) / entry_price_short * 100.0
                    trades[current_short_idx]['mae_pct'] = (entry_price_short - eh) / entry_price_short * 100.0

                stop_reason_short = 'BE Stop' if abs(float(effective_stop_short) - float(entry_price_short)) <= max(0.005, float(entry_price_short)*1e-4) else 'Stop Loss'
                log_trade_event(ticker, 'EXIT', direction='SHORT', price=effective_stop_short, shares=remaining_shares_short,
                                reason=stop_reason_short, timestamp=ts, balance_change=loss,
                                pnl_pct=(entry_price_short - effective_stop_short)/entry_price_short * 100,
                                stop_at_event=effective_stop_short, trade_id=current_short_id)
                in_short = False
                open_positions = max(0, open_positions - 1)
                trades[current_short_idx].update({'exit_time': ts, 'exit': effective_stop_short, 'reason': 'stop',
                                                  'balance': current_balance, 'remaining_shares': 0, 'mark_price': effective_stop_short})
            else:
                while next_partial_index_short < len(PARTIAL_LEVELS):
                    move_pct   = PARTIAL_LEVELS[next_partial_index_short]['move_pct']
                    exit_frac  = PARTIAL_LEVELS[next_partial_index_short]['exit_fraction']
                    target_price = entry_price_short * (1 - move_pct)
                    if l <= target_price:
                        fill_price = target_price
                        shares_to_sell = remaining_shares_short * exit_frac
                        cover_cost = shares_to_sell * fill_price
                        if USE_DYNAMIC_CASH:
                            available_cash -= cover_cost
                        profit = shares_to_sell * (entry_price_short - fill_price)
                        current_balance += profit
                        remaining_shares_short -= shares_to_sell
                        trades[current_short_idx]['remaining_shares'] = remaining_shares_short
                        log_trade_event(ticker, 'PARTIAL', direction='SHORT', price=fill_price, shares=shares_to_sell,
                                        reason=f'Partial level {next_partial_index_short + 1}',
                                        timestamp=ts, balance_change=profit,
                                        pnl_pct=(entry_price_short - fill_price)/entry_price_short * 100,
                                        stop_at_event=stop_short, trade_id=current_short_id)
                        next_partial_index_short += 1
                        # v13: mark SHORT Partial 2 time
                        if next_partial_index_short == 2 and p2_time_short is None:
                            p2_time_short = ts
                            debug_print('P2 reached (SHORT) → starting opposite lock window', 2, ticker)
                        # v13: BE activation after required partials and min minutes (when enabled)
                        _elapsed_min = (ts - entry_time_short).total_seconds() / 60.0 if entry_time_short is not None else 0.0
                        _need_partials = V13_BE_AFTER_PARTIALS if ENABLE_V13_IMPROVEMENTS else 1
                        _need_minutes  = V13_BE_MIN_MINUTES if ENABLE_V13_IMPROVEMENTS else 0.0
                        if (not be_set_short) and (next_partial_index_short >= _need_partials) and (_elapsed_min >= _need_minutes):
                            stop_short = entry_price_short
                            trades[current_short_idx]['stop'] = stop_short
                            be_set_short = True
                            log_trade_event(ticker, 'BE', direction='SHORT', price=stop_short, shares=remaining_shares_short,
                                            reason='Breakeven activated', timestamp=ts,
                                            pnl_pct=(entry_price_short - stop_short)/entry_price_short * 100,
                                            stop_at_event=stop_short, trade_id=current_short_id)
                        # one-partial-per-bar pacing ← ADDED
                        if not ALLOW_MULTI_PARTIALS_PER_BAR:
                            break     
                    else:
                        break
                # --- Step ratchet (SHORT) ---
                _prev_stop = stop_short
                stop_short, ratchet_index_short = _apply_step_ratchet_short(
                    entry_price_short, l, stop_short, be_set_short, ratchet_index_short
                )
                if stop_short is not None and _prev_stop is not None and stop_short < _prev_stop - 1e-6:
                    trades[current_short_idx]['stop'] = stop_short
                    log_trade_event(ticker, 'RATCHET', direction='SHORT', price=stop_short, shares=remaining_shares_short,
                                    reason=f'Stop lowered to {((entry_price_short/stop_short)-1)*100:.2f}% cushion',
                                    timestamp=ts, stop_at_event=stop_short, trade_id=current_short_id)

                if not tp_reached_short and entry_price_short is not None:
                    if l <= entry_price_short * (1 - tp_pct):
                        tp_reached_short = True

                if enableTimeout and entry_time_short is not None:
                    elapsed_min = (ts - entry_time_short).total_seconds() / 60.0
                    timeout_limit = float(trades[current_short_idx].get('timeout_min', exitTimeout))
                    entry_tier = trades[current_short_idx].get('entry_tier')
                    # --- HA vs Timeout precedence ---
                    def _ha_trend_favors(side: str, ts_check):
                        try:
                            closed_bucket = tf_floor(ts, HA_TIMEFRAME) - tf_delta(HA_TIMEFRAME)
                            if closed_bucket in ha_tf.index:
                                ha_open = ha_tf.loc[closed_bucket, 'ha_open']
                                ha_close = ha_tf.loc[closed_bucket, 'ha_close']
                                ha_curr_bullish = ha_close > ha_open
                                if side == 'LONG':
                                    return ha_curr_bullish
                                else:
                                    return not ha_curr_bullish
                        except Exception:
                            return False
                        return False
                    suppress_timeout = False
                    if HA_OVERRIDES_TIMEOUT and enableHAExit:
                        tp_ok = (not HA_OVERRIDE_REQUIRE_TP) or bool(tp_reached_short)
                        if tp_ok and _ha_trend_favors('SHORT', ts):
                            suppress_timeout = True
                    if elapsed_min > timeout_limit and not exit_block_active_short:
                        if suppress_timeout:
                            debug_print(f"Timeout {elapsed_min:.2f}m suppressed by HA trend (still favorable)", 2, ticker)
                        else:
                            timeout_reason = f"Timeout (Tier {entry_tier}, {int(round(timeout_limit))}m)" if ENABLE_TIER_AWARE_TIMEOUT and entry_tier in ('A','B') else "Timeout"
                            debug_print(f"SHORT timeout exit after {elapsed_min:.2f} min (limit {timeout_limit:.2f}m)", 2, ticker)
                            cover_cost = remaining_shares_short * c
                            if USE_DYNAMIC_CASH:
                                available_cash -= cover_cost
                            profit = remaining_shares_short * (entry_price_short - c)
                            current_balance += profit

                            if ENABLE_MAE_MFE:
                                eh = trades[current_short_idx].get('extreme_high', entry_price_short)
                                el = trades[current_short_idx].get('extreme_low', entry_price_short)
                                trades[current_short_idx]['mfe_pct'] = (entry_price_short - el) / entry_price_short * 100.0
                                trades[current_short_idx]['mae_pct'] = (entry_price_short - eh) / entry_price_short * 100.0

                            log_trade_event(ticker, 'EXIT', direction='SHORT', price=c, shares=remaining_shares_short,
                                            reason=timeout_reason, timestamp=ts, balance_change=profit,
                                            pnl_pct=(entry_price_short - c)/entry_price_short * 100,
                                            stop_at_event=stop_short, trade_id=current_short_id)
                            in_short = False
                            open_positions = max(0, open_positions - 1)
                            trades[current_short_idx].update({'exit_time': ts, 'exit': c, 'reason': 'timeout',
                                                              'balance': current_balance, 'remaining_shares': 0, 'mark_price': c})

                exit_block_active_short = False

    if REPLAY_DELAY:
        time.sleep(REPLAY_DELAY)

    # --- FORCE CLOSE ALL REMAINING OPEN TRADES AT END-OF-DAY ---
    try:
        last_ts = df.index[-1]
        last_price = float(df['close'].iloc[-1])
    except Exception:
        last_ts = None
        last_price = None

    for idx, t in enumerate(trades):
        if t['ticker'] != ticker:
            continue
        rs = t.get('remaining_shares', 0)
        if rs and rs > 0:
            direction = t.get('direction')
            entry_price = t.get('entry')
            price_used = last_price if last_price is not None else entry_price
            if direction == 'LONG':
                proceeds = rs * price_used
                if USE_DYNAMIC_CASH:
                    available_cash += proceeds
                pnl = rs * (price_used - entry_price)
                current_balance += pnl

                if ENABLE_MAE_MFE:
                    eh = max(t.get('extreme_high', entry_price), price_used)
                    el = min(t.get('extreme_low', entry_price), price_used)
                    trades[idx]['mfe_pct'] = (eh - entry_price) / entry_price * 100.0
                    trades[idx]['mae_pct'] = (el - entry_price) / entry_price * 100.0

                log_trade_event(
                    ticker, 'EXIT', direction='LONG', price=price_used, shares=rs,
                    reason='market_close', timestamp=last_ts,
                    balance_change=pnl,
                    pnl_pct=(price_used - entry_price) / entry_price if entry_price else None,
                    stop_at_event=t.get('stop'),
                    trade_id=t.get('trade_id')
                )
            elif direction == 'SHORT':
                cover_cost = rs * price_used
                if USE_DYNAMIC_CASH:
                    available_cash -= cover_cost
                pnl = rs * (entry_price - price_used)
                current_balance += pnl

                if ENABLE_MAE_MFE:
                    eh = max(t.get('extreme_high', entry_price), price_used)
                    el = min(t.get('extreme_low', entry_price), price_used)
                    trades[idx]['mfe_pct'] = (entry_price - el) / entry_price * 100.0
                    trades[idx]['mae_pct'] = (entry_price - eh) / entry_price * 100.0

                log_trade_event(
                    ticker, 'EXIT', direction='SHORT', price=price_used, shares=rs,
                    reason='market_close', timestamp=last_ts,
                    balance_change=pnl,
                    pnl_pct=(entry_price - price_used) / entry_price if entry_price else None,
                    stop_at_event=t.get('stop'),
                    trade_id=t.get('trade_id')
                )
            trades[idx].update({
                'exit_time': last_ts,
                'exit': price_used,
                'reason': 'market_close',
                'balance': current_balance,
                'remaining_shares': 0,
                'mark_price': price_used
            })
            open_positions = max(0, open_positions - 1)

    if entry_attempts == 0:
        log_print(f"{ticker}: No data within entry window.", 1)
    elif not useMACD and not alwaysEnter:
        log_print(f"{ticker}: No signal logic enabled.", 1)

# ----------------- REPORT (formatted) -----------------
def print_final_report():
    # === 20–30 MIN HOLD COHORT (optional) ===
    if 'SHOW_COHORT_20_30' in globals() and SHOW_COHORT_20_30:
        _lo, _hi = COHORT_20_30_WINDOW
        try:
            # Use the same trade table you already build (_dfT). It should contain per-trade P&L and hold minutes.
            _src = _dfT.copy()
        except NameError:
            _src = None

        if _src is not None and not _src.empty:
            # Try to be resilient to column names in your script
            _hold_col = 'hold_min' if 'hold_min' in _src.columns else ('hold_minutes' if 'hold_minutes' in _src.columns else None)
            _pnl_col  = 'pnl_amount' if 'pnl_amount' in _src.columns else ('pnl' if 'pnl' in _src.columns else None)
            _dir_col  = 'direction' if 'direction' in _src.columns else 'Direction'
            _exit_col = 'exit_reason_norm2' if 'exit_reason_norm2' in _src.columns else ('exit_reason' if 'exit_reason' in _src.columns else 'Exit Reason')

            if _hold_col and _pnl_col and _dir_col and _exit_col and {'ticker'}.issubset(_src.columns):
                _cohort = _src.loc[
                    (_src[_hold_col] >= _lo) & (_src[_hold_col] < _hi),
                    ['ticker', _dir_col, _exit_col, _pnl_col, _hold_col]
                ].copy()

                if not _cohort.empty:
                    # Pretty headers
                    _cohort = _cohort.rename(columns={
                        _dir_col: 'Direction',
                        _exit_col: 'Exit Reason',
                        _pnl_col: 'P&L $',
                        _hold_col: 'Hold (min)'
                    })
                    # Sort by hold or P&L as you like
                    _cohort = _cohort.sort_values(['Hold (min)', 'P&L $'], ascending=[True, False])

                    print("\n=== 20–30 MIN HOLD COHORT ===")
                    print(_cohort.to_string(index=False, formatters={'P&L $': lambda v: f"${v:,.2f}"}))
                else:
                    print("\n=== 20–30 MIN HOLD COHORT ===")
                    print("(none)")
            else:
                # Columns missing: skip quietly
                pass
    def tsfmt(series):
        try:
            series = series.dt.tz_convert(tz_et)
            out = series.dt.strftime('%-m/%-d %H:%M')
        except Exception:
            series = series.dt.tz_convert(tz_et)
            out = series.dt.strftime('%m/%d %H:%M')
        return out.fillna('')

    def f2(x):
        return "" if pd.isna(x) else f"{float(x):.2f}"

    print("\n=== TRADE SUMMARY ===")
    if trades:
        df_trades = pd.DataFrame(trades)

        for col, default in [('exit_time', pd.NaT), ('exit', np.nan), ('reason', np.nan),
                             ('mfe_pct', np.nan), ('mae_pct', np.nan)]:
            if col not in df_trades.columns:
                df_trades[col] = default

        # bars_held in MINUTES regardless of bar size
        df_trades['bars_held'] = np.nan
        mask_has_exit = df_trades['exit_time'].notna()
        if mask_has_exit.any():
            et_entry = df_trades['entry_time'].dt.tz_convert(tz_et)
            et_exit = df_trades['exit_time'].dt.tz_convert(tz_et)
            df_trades.loc[mask_has_exit, 'bars_held'] = (
                (et_exit[mask_has_exit] - et_entry[mask_has_exit]).dt.total_seconds() / 60.0
            )

        df_show = df_trades.copy()
        df_show = df_show.rename(columns={'remaining_shares': 'rem_shares', 'trade_amount': 'size$'})

        df_show['entry_time'] = tsfmt(df_trades['entry_time'])
        if 'exit_time' in df_trades.columns:
            df_show['exit_time'] = tsfmt(df_trades['exit_time'])

        def map_reason(s):
            if pd.isna(s):
                return ""
            s = str(s)
            s = s.replace('Heiken Ashi Reversal', 'HA')
            s = s.replace('Heiken Ashi Reversal (1m)', 'HA (1m)')
            s = s.replace('Heiken Ashi Reversal (5m)', 'HA (5m)')
            s = s.replace('Breakeven activated', 'BE Activated')
            s = pd.Series([s]).str.replace(r'Partial level (\d+)', r'Partial \1', regex=True).iloc[0]
            return s

        df_show['reason'] = df_show['reason'].apply(map_reason)

        for col in ['entry', 'exit', 'stop']:
            if col in df_show.columns:
                df_show[col] = df_show[col].apply(f2)

        if 'rem_shares' in df_show.columns:
            df_show['rem_shares'] = df_show['rem_shares'].apply(f2)
        if 'balance' in df_show.columns:
            df_show['balance'] = df_show['balance'].apply(f2)
        if 'size$' in df_show.columns:
            df_show['size$'] = df_show['size$'].apply(lambda x: "" if pd.isna(x) else f"{round(x):.0f}")

        print(df_show[['ticker', 'direction', 'entry_time', 'entry',
                       'exit_time', 'exit', 'size', 'rem_shares', 'stop',
                       'reason', 'balance', 'size$', 'bars_held', 'mfe_pct', 'mae_pct']].to_string(index=False))

        print("\n=== TRADE LOGS ===")
        df_logs = pd.DataFrame(trade_logs)
        if not df_logs.empty:
            try:
                df_logs['timestamp'] = df_logs['timestamp'].dt.tz_convert(tz_et).dt.strftime('%-m/%-d %H:%M')
            except Exception:
                df_logs['timestamp'] = df_logs['timestamp'].dt.tz_convert(tz_et).dt.strftime('%m/%d %H:%M')

            df_logs = df_logs.merge(df_trades[['trade_id', 'sl_pct']], on='trade_id', how='left')
            df_logs['sl_pct'] = df_logs['sl_pct'].apply(lambda v: f"{v*100:.0f}%" if pd.notna(v) else "")

            df_logs = df_logs.rename(columns={'trade_amount': 'size$'})

            def map_reason2(s):
                if pd.isna(s):
                    return ""
                s = str(s)
                s = s.replace('Heiken Ashi Reversal', 'HA')
                s = s.replace('Heiken Ashi Reversal (1m)', 'HA (1m)')
                s = s.replace('Heiken Ashi Reversal (5m)', 'HA (5m)')
                s = s.replace('Breakeven activated', 'BE Activated')
                s = pd.Series([s]).str.replace(r'Partial level (\d+)', r'Partial \1', regex=True).iloc[0]
                return s

            df_logs['reason'] = df_logs['reason'].apply(map_reason2)

            for col in ['price', 'stop']:
                if col in df_logs.columns:
                    df_logs[col] = df_logs[col].apply(f2)
            if 'shares' in df_logs.columns:
                df_logs['shares'] = df_logs['shares'].apply(f2)
            if 'pnl_pct' in df_logs.columns:
                df_logs['pnl_pct'] = df_logs['pnl_pct'].apply(lambda x: "" if pd.isna(x) else f"{float(x):.2f}")
            if 'size$' in df_logs.columns:
                df_logs['size$'] = df_logs['size$'].apply(lambda x: "" if pd.isna(x) else f"{round(x):.0f}")

            print(df_logs[['timestamp','ticker','event','direction','price','shares',
                           'stop','sl_pct','reason','balance_change','pnl_pct','size$']].to_string(index=False))

            # --- Daily Direction P&L (from logs) ---
            if 'SHOW_DAILY_DIR_PNL' in globals() and SHOW_DAILY_DIR_PNL:
                _logs = df_logs.copy()

                # Only rows that actually change P&L
                _logs['bc'] = pd.to_numeric(_logs.get('balance_change', 0), errors='coerce').fillna(0.0)
                _logs = _logs[_logs['event'].isin(['PARTIAL', 'EXIT'])]
                # Keep LONG / SHORT only
                _logs = _logs[_logs['direction'].isin(['LONG', 'SHORT'])]

                # Pull date part from the ET timestamp string like "7/1 09:30"
                try:
                    _logs['date'] = _logs['timestamp'].str.split().str[0]
                except Exception:
                    # If timestamp isn’t a string yet, fall back to parsing
                    _logs['date'] = pd.to_datetime(_logs['timestamp'], errors='coerce').dt.tz_convert(tz_et).dt.strftime('%-m/%-d')

                if not _logs.empty and 'date' in _logs.columns:
                    # Build per-day / per-direction Profit, Loss, Net
                    def _sum_pos(g): return g[g['bc'] > 0].groupby('direction')['bc'].sum()
                    def _sum_neg(g): return g[g['bc'] < 0].groupby('direction')['bc'].sum()
                    def _sum_all(g): return g.groupby('direction')['bc'].sum()

                    for _day, _g in _logs.groupby('date', sort=True):
                        # sums per direction
                        _p = _g[_g['bc'] > 0].groupby('direction')['bc'].sum()   # positive P&L only
                        _l = _g[_g['bc'] < 0].groupby('direction')['bc'].sum()   # negative P&L only
                        _n = _g.groupby('direction')['bc'].sum()                 # net

                        def _fmt(v):
                            try: return f"${float(v):,.2f}"
                            except: return "$0.00"

                        print(f"\n--- { _day } — Daily Direction P&L ---")
                        print("Direction   Profit $      Loss $        Net $")

                        # per-side rows (LONG, SHORT)
                        for _side in ['LONG','SHORT']:
                            p = _p.get(_side, 0.0)
                            l = _l.get(_side, 0.0)
                            n = _n.get(_side, 0.0)
                            print(f"{_side:<9} {_fmt(p):>10}  {_fmt(l):>10}  {_fmt(n):>10}")

                        # TOTAL row across both directions
                        p_tot = float(_p.sum()) if len(_p) else 0.0
                        l_tot = float(_l.sum()) if len(_l) else 0.0
                        n_tot = float(_n.sum()) if len(_n) else 0.0
                        print(f"{'Total':<9} {_fmt(p_tot):>10}  {_fmt(l_tot):>10}  {_fmt(n_tot):>10}")
        else:
            print("(no trade logs)")

        
        # === STOP-LOSS REPORTS (toggleable) ===
        if 'SHOW_STOPLOSS_TRADES' in globals() and SHOW_STOPLOSS_TRADES:
            try:
                _sl = df_logs[(df_logs.get('event','')=='EXIT') & (df_logs.get('reason','').astype(str).str.strip().str.lower()=='stop loss')].copy()
                print("\n=== STOP-LOSS TRADES (Ticker × Date/Time ET) ===")
                if _sl.empty:
                    print("(none)")
                else:
                    _keep = [c for c in ['ticker','timestamp','direction','price','pnl_pct','size$'] if c in _sl.columns]
                    print(_sl[_keep].to_string(index=False))

                if 'SHOW_STOPLOSS_UNIQUE' in globals() and SHOW_STOPLOSS_UNIQUE and not _sl.empty:
                    try:
                        _dates = _sl['timestamp'].astype(str).str.split().str[0]
                        _uniq = _sl.assign(date=_dates)[['ticker','date']].drop_duplicates()
                        print("\nTickers × Dates (unique, Stop Loss):")
                        print(_uniq.to_string(index=False))
                    except Exception:
                        pass

                if 'SHOW_STOPLOSS_OFFENDERS' in globals() and SHOW_STOPLOSS_OFFENDERS and not _sl.empty:
                    try:
                        _off = _sl.groupby('ticker').size().reset_index(name='stop_losses').sort_values('stop_losses', ascending=False)
                        print("\nTop Stop-Loss Offenders (by count):")
                        print(_off.to_string(index=False))
                    except Exception:
                        pass
            except Exception as _e:
                debug_print(f"Stop-loss reporting error: {type(_e).__name__}: {_e}", 1, 'REPORT')
    # ==== PERFORMANCE SUMMARY ====
        total_trades = len(df_trades)
        profitable_trades = sum(1 for t in trades if t.get('exit') is not None and t.get('direction') == 'LONG' and t['exit'] > t['entry']) + \
                            sum(1 for t in trades if t.get('exit') is not None and t.get('direction') == 'SHORT' and t['exit'] < t['entry'])
        losing_trades = sum(1 for t in trades if t.get('exit') is not None and t.get('direction') == 'LONG' and t['exit'] < t['entry']) + \
                        sum(1 for t in trades if t.get('exit') is not None and t.get('direction') == 'SHORT' and t['exit'] > t['entry'])
        breakeven_trades = total_trades - profitable_trades - losing_trades

        win_rate = (profitable_trades / total_trades * 100.0) if total_trades else 0.0
        loss_rate = (losing_trades / total_trades * 100.0) if total_trades else 0.0
        breakeven_rate = (breakeven_trades / total_trades * 100.0) if total_trades else 0.0
        total_pnl = current_balance - EQUITY

        if 'SHOW_PERFORMANCE_SUMMARY' in globals() and SHOW_PERFORMANCE_SUMMARY:
            print("\n=== PERFORMANCE SUMMARY ===")
            print(f"Total Trades: {total_trades}")
            print(f"Profitable Trades: {profitable_trades}")
            print(f"Win Rate: {win_rate:.1f}%")
            print(f"Losing Trades: {losing_trades}")
            print(f"Loss Rate: {loss_rate:.1f}%")
            print(f"Breakeven Trades: {breakeven_trades}")
            print(f"Breakeven Rate: {breakeven_rate:.1f}%")
            print(f"Total Profit and Loss: ${total_pnl:,.2f}")
        # ==== EXTENDED RESULTS (optional) ====
            if 'SHOW_RESULT_TABLES' in globals() and SHOW_RESULT_TABLES and total_trades:
                import numpy as _np
                import pandas as _pd
                _df_logs = _pd.DataFrame(trade_logs) if 'trade_logs' in globals() else _pd.DataFrame()
                if not _df_logs.empty and 'balance_change' in _df_logs.columns:
                    _df_logs['balance_change'] = _pd.to_numeric(_df_logs['balance_change'], errors='coerce').fillna(0.0)
                _pnl_by_trade = _df_logs.groupby('trade_id', dropna=True)['balance_change'].sum() if not _df_logs.empty else _pd.Series(dtype=float)
                _dfT = df_trades.copy()
                for col in ['trade_id','direction','entry','exit','trade_amount','reason','bars_held']:
                    if col not in _dfT.columns: _dfT[col] = _np.nan
                _dfT['gross_pnl$'] = _dfT['trade_id'].map(_pnl_by_trade).fillna(0.0)
                _eps = 0.01
                _dfT['outcome'] = _pd.cut(_dfT['gross_pnl$'],[-_np.inf,-_eps,_eps,_np.inf],labels=['Loss','BE','Profit'],include_lowest=True).astype(str)
                _partials = _df_logs[_df_logs.get('event','')=='PARTIAL'].groupby('trade_id').size() if not _df_logs.empty else _pd.Series(dtype=int)
                _dfT['partials_count'] = _dfT['trade_id'].map(_partials).fillna(0).astype(int)
                _dfT['took_partial'] = _dfT['partials_count'] > 0
            if 'SHOW_DIRECTION_SUMMARY' in globals() and SHOW_DIRECTION_SUMMARY:
                _rows = []
                for _dir in ['LONG','SHORT']:
                    _sub = _dfT[_dfT['direction'].astype(str).str.upper()==_dir]
                    if _sub.empty:
                        _rows.append((_dir.title(),0,0,0,0,0.0,0.0,0.0,0.0,_np.nan,_np.nan,_np.nan,0,_np.nan)); continue
                    _trades=len(_sub); _wins=int((_sub['outcome']=='Profit').sum()); _losses=int((_sub['outcome']=='Loss').sum()); _bes=int((_sub['outcome']=='BE').sum())
                    _winrate=_wins/_trades*100.0 if _trades else 0.0
                    _gprofit=float(_sub.loc[_sub['outcome']=='Profit','gross_pnl$'].sum()); _gloss=float(_sub.loc[_sub['outcome']=='Loss','gross_pnl$'].sum())
                    _net=float(_sub['gross_pnl$'].sum()); _avg=float(_sub['gross_pnl$'].mean())
                    _avg_pct=float(_sub['pnl_pct'].mean()) if _sub['pnl_pct'].notna().any() else _np.nan
                    _med_pct=float(_sub['pnl_pct'].median()) if _sub['pnl_pct'].notna().any() else _np.nan
                    _with_partial=int((_sub['took_partial']).sum()); _avg_hold=float(_sub['bars_held'].mean()) if 'bars_held' in _sub.columns else _np.nan
                    _rows.append((_dir.title(),_trades,_wins,_losses,_bes,_winrate,_gprofit,_gloss,_net,_avg,_avg_pct,_med_pct,_with_partial,_avg_hold))
                _sub=_dfT; _trades=len(_sub); _wins=int((_sub['outcome']=='Profit').sum()); _losses=int((_sub['outcome']=='Loss').sum()); _bes=int((_sub['outcome']=='BE').sum())
                _winrate=_wins/_trades*100.0 if _trades else 0.0
                _gprofit=float(_sub.loc[_sub['outcome']=='Profit','gross_pnl$'].sum()); _gloss=float(_sub.loc[_sub['outcome']=='Loss','gross_pnl$'].sum())
                _net=float(_sub['gross_pnl$'].sum()); _avg=float(_sub['gross_pnl$'].mean())
                _avg_pct=float(_sub['pnl_pct'].mean()) if _sub['pnl_pct'].notna().any() else _np.nan
                _med_pct=float(_sub['pnl_pct'].median()) if _sub['pnl_pct'].notna().any() else _np.nan
                _with_partial=int((_sub['took_partial']).sum()); _avg_hold=float(_sub['bars_held'].mean()) if 'bars_held' in _sub.columns else _np.nan
                _rows.append(('All',_trades,_wins,_losses,_bes,_winrate,_gprofit,_gloss,_net,_avg,_avg_pct,_med_pct,_with_partial,_avg_hold))
                _dir_cols=['Direction','Trades','Winners','Losers','Breakevens','Win Rate %','Gross Profit $','Gross Loss $','Net P&L $','Avg P&L $','Avg P&L %','Median P&L %','Trades w/ Partial','Avg Hold (min)']
                _dir_df=_pd.DataFrame(_rows,columns=_dir_cols)
                def _m(x): 
                    try: return f"${float(x):,.2f}"
                    except: return ""
                def _p(x): 
                    try: return f"{float(x):.2f}"
                    except: return ""
                for col in ['Gross Profit $','Gross Loss $','Net P&L $','Avg P&L $']: _dir_df[col]=_dir_df[col].apply(_m)
                for col in ['Win Rate %','Avg P&L %','Median P&L %']: _dir_df[col]=_dir_df[col].apply(_p)

                print("=== DIRECTION SUMMARY ==="); print(_dir_df.to_string(index=False))
            if 'SHOW_EXIT_BREAKDOWN' in globals() and SHOW_EXIT_BREAKDOWN:
                import pandas as _pd
                def _norm_reason(r):
                    raw = (r or '').strip()
                    s = raw.lower()
                    # keep flavored timeouts as-is, so they become distinct rows
                    if s.startswith('timeout (tier'):
                        return raw
                    if 'heiken ashi' in s or 'ha' in s:
                        return 'HA'
                    if 'timeout' in s:
                        return 'Timeout'
                    if 'be stop' in s:
                        return 'BE Stop'
                    if 'stop loss' in s or s == 'stop':
                        return 'Stop Loss'
                    if 'breakeven activated' in s or 'be activated' in s:
                        return 'BE'
                    if 'partial' in s:
                        return 'Partial'
                    return raw.title() if raw else ''
                _dfT['exit_reason_norm'] = _dfT['reason'].apply(_norm_reason)
                # --- Split stops into BE Stop vs Stop Loss (report-only) ---
                # trades that ever triggered BE (from trade_logs)
                _be_ids = set(_df_logs.loc[_df_logs.get('event','') == 'BE', 'trade_id']) if not _df_logs.empty else set()
                _dfT['be_ever'] = _dfT['trade_id'].isin(_be_ids)

                # tolerance to avoid float/tick noise: max($0.005, entry * 1e-4)
                _eps_series = _np.maximum(0.005, _dfT['entry'].astype(float) * 1e-4)

                def _split_stop(row):
                    # keep non-stop reasons as-is
                    if row['exit_reason_norm'] != 'Stop':
                        return row['exit_reason_norm']
                    # if BE ever triggered and final stop equals entry (within eps), call it BE Stop
                    if bool(row.get('be_ever', False)):
                        try:
                            if abs(float(row['exit']) - float(row['entry'])) <= float(_eps_series.loc[row.name]):
                                return 'BE Stop'
                        except Exception:
                            pass
                    # otherwise it's a true loss stop
                    return 'Stop Loss'

                _dfT['exit_reason_norm2'] = _dfT.apply(_split_stop, axis=1)

                # --- Preserve flavored timeout after stop split ---
                def _keep_timeout_flavor(row):
                    if row['exit_reason_norm2'] == 'Timeout':
                        raw = str(row.get('reason', '')).strip()
                        if raw.lower().startswith('timeout (tier'):
                            return raw   # keep the flavored version
                    return row['exit_reason_norm2']
                _dfT['exit_reason_norm2'] = _dfT.apply(_keep_timeout_flavor, axis=1)
                # use a new display order that includes the split stop types and flavored timeouts
                _order_reason = [
                    'HA',
                    f'Timeout (Tier A, {int(round(TIMEOUT_TIER_A_MIN))}m)',
                    f'Timeout (Tier B, {int(round(TIMEOUT_TIER_B_MIN))}m)',
                    'Timeout',
                    'Take Profit',
                    'BE Stop',
                    'Stop Loss',
                    'Market Close',
                    'Unknown'
                ]

                _counts = _dfT.pivot_table(index=['direction','exit_reason_norm2'], columns='outcome', values='trade_id', aggfunc='count', fill_value=0).reset_index()
                _dollars = _dfT.groupby(['direction','exit_reason_norm2'])['gross_pnl$'].sum().reset_index(name='Net $')
                _profit_dollars = _dfT[_dfT['outcome']=='Profit'].groupby(['direction','exit_reason_norm2'])['gross_pnl$'].sum().reset_index().rename(columns={'gross_pnl$':'Profit $'})
                _loss_dollars = _dfT[_dfT['outcome']=='Loss'].groupby(['direction','exit_reason_norm2'])['gross_pnl$'].sum().reset_index().rename(columns={'gross_pnl$':'Loss $'})
                _tbl=_counts.merge(_profit_dollars,how='left').merge(_loss_dollars,how='left').merge(_dollars,how='left')
                # --- Ensure missing outcome columns exist even if the pivot didn't create them ---
                for _col in ['Profit', 'BE', 'Loss']:
                    if _col not in _tbl.columns:
                        _tbl[_col] = 0
                # --- Ensure dollar columns exist & are numeric before formatting ---
                for _col in ['Profit $', 'Loss $', 'Net $']:
                    if _col not in _tbl.columns:
                        _tbl[_col] = 0.0
                    _tbl[_col] = _pd.to_numeric(_tbl[_col], errors='coerce').fillna(0.0)
                def _m2(x):
                    try: return f"${float(x):,.2f}"
                    except: return ""
                for col in ['Profit $','Loss $','Net $']:
                    if col in _tbl.columns:
                        _tbl[col]=_tbl[col].apply(_m2)
                _tbl=_tbl.rename(columns={'direction':'Direction','exit_reason_norm2':'Exit Reason','Profit':'Profit #','Loss':'Loss #','BE':'BE #'})
                if 'SHOW_EXIT_REASON_OUTCOME' in globals() and SHOW_EXIT_REASON_OUTCOME:
                    print("=== EXIT REASON × OUTCOME ===")
                    print(_tbl[['Direction','Exit Reason','Profit #','BE #','Loss #','Profit $','Loss $','Net $']].to_string(index=False))

                    # ==== CONFIGURATION SUMMARY (END) ====

                if SHOW_CONFIG_SUMMARY:
                    masked_key = (API_KEY[:10] + '...') if API_KEY else 'Not Set'
                    interval_desc = (
                        f"1 second (local; fills at minute open; HA exit on {HA_TIMEFRAME})"
                        if DATA_SOURCE == 'databento_local'
                        else f"{INTERVAL_MULTIPLIER} {INTERVAL_TIMESPAN}"
                    )
                    rows = [

                        ('API Key', masked_key, 'Polygon.io API key (masked)'),

                        ('Debug Level', str(DEBUG_LEVEL), 'Debug logging level (0-3)'),

                        ('Log Level', str(LOG_LEVEL), 'Log level (0-3)'),

                ('Entry Time', ENTRY_TIME_ET, 'Time for trade entries (ET)'),
                ('Entry Window', f"{entryWindowMinutes} minutes", 'Minutes after entry time to accept entries'),
                ('DATA_SOURCE', DATA_SOURCE, 'polygon or databento_local'),
                ('DATA_DB_DIR', DATA_DB_DIR if DATA_SOURCE=='databento_local' else '-', 'Local DB folder'),
                ('DB Dataset/Schema', f"{DB_DATASET}/{DB_SCHEMA}" if DATA_SOURCE=='databento_local' else '-', 'Local dataset/schema'),
                ('Interval', interval_desc, 'Granularity & fill/exit behavior'),
                ('HA Timeframe', HA_TIMEFRAME, 'Heiken-Ashi exit timeframe'),
                ('Long Enable', str(longEnable), 'Allow long trades'),
                ('Short Enable', str(shortEnable), 'Allow short trades'),
                ('MACD Enabled', str(useMACD), 'Use MACD for entry signals'),
                ('Always Enter', str(alwaysEnter), 'Bypass MACD and enter regardless'),
                ('ATR Period', str(atrPeriod), 'ATR lookback length (bars)'),
                ('ATR Entry?', str(enableATREntry), 'Gate entries by ATR'),
                ('ATR Stop?', str(enableATRStop), 'Use ATR-based stop'),
                ('Tier Recent Lookback', str(recentCrossLookback), 'MACD recent-cross lookback'),
                ('Allow Late-Trend Entries', str(allowLateTrendEntries), 'Enable Tier B entries'),
                ('Late-Trend ATR Multiplier', f"{lateTrendATRMultiplier:.2f}", 'ATR multiplier for Tier B'),
                ('Late-Trend Size', f"{lateTrendSize*100:.2f}%", 'Position size used for Tier B'),
                ('Late-Trend SL %', f"{lateTrendSlPct*100:.2f}%", 'Stop % used for Tier B'),
                ('Limit One Per Direction', str(limitOnePerDirection), 'At most one LONG and one SHORT'),
                ('USE_DYNAMIC_CASH', str(USE_DYNAMIC_CASH), 'Dynamic cash accounting'),
                ('ALLOW_CONCURRENT_POS', str(ALLOW_CONCURRENT_POSITIONS), 'Allow multiple positions'),
                ('Timeout (min)', str(exitTimeout), 'Exit timeout measured in minutes'),
            ]
            print("\n=== CONFIGURATION SUMMARY ===")
            print(f"+{'-'*30}+{'-'*20}+{'-'*50}+")
            print(f"| {'Setting':<28} | {'Value':<18} | {'Description':<48} |")
            print(f"+{'-'*30}+{'-'*20}+{'-'*50}+")
            for s, v, d in rows:
                print(f"| {s:<28} | {v:<18} | {d:<48} |")
            print(f"+{'-'*30}+{'-'*20}+{'-'*50}+")
    else:
        print("(no trades)")

# ----------------- MAIN -----------------
if __name__ == '__main__':
    ticker_dates = parse_csv(CSV_FILE)
    if not ticker_dates:
        log_print("No tickers in CSV.", 1)
        sys.exit(0)

    for ticker, iso_date in ticker_dates:
        entry_local = datetime.fromisoformat(f"{iso_date} {ENTRY_TIME_ET}")
        entry_dt = tz_et.localize(entry_local).astimezone(pytz.UTC)
        entry_window_end = entry_dt + timedelta(minutes=entryWindowMinutes)
        run_ticker(ticker, iso_date)

    print_final_report()
