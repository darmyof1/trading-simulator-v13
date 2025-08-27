# src/modules/datafeeds/alpaca_data.py
from __future__ import annotations
import os
import json
import signal
import asyncio
import argparse
from typing import Callable, Optional, List, Dict, Any
import pandas as pd
import websockets
from time import monotonic
from collections import defaultdict
import requests
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from modules.datafeeds.common import load_symbols, ensure_out_dir, IngressMode, default_out
from modules.storage.writers import write_ohlcv_csv, save_parquet_append, parquet_sessions, close_parquet_sessions


# --- safe UTC helper (prefer shared util if present) ---
try:
    from modules.utils.time import to_utc  # if you created this already
except Exception:
    def to_utc(ts):
        t = pd.Timestamp(ts)
        return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")


# --- ACK helpers --------------------------------------------------------------
def _auth_ok(msg) -> bool:
    """
    Accept both old and new Alpaca WS shapes, and arrays of messages.
    Examples seen:
      {"T":"success","msg":"authenticated"}
      {"stream":"authorization","data":{"action":"auth","status":"authorized"}}
      [{"T":"success","msg":"authenticated"}, {"T":"subscription",...}]
    """
    if isinstance(msg, list):
        return any(_auth_ok(m) for m in msg)
    if not isinstance(msg, dict):
        return False
    if msg.get("msg") == "authenticated":
        return True
    if msg.get("T") == "success" and msg.get("msg") == "authenticated":
        return True
    if msg.get("status") == "authorized":
        return True
    if msg.get("stream") == "authorization" and msg.get("data", {}).get("status") == "authorized":
        return True
    return False

def _sub_ok(msg) -> bool:
    """
    Subscription acks show up as a dict or inside an array. Examples:
      {"T":"subscription","trades":["TSLA"],"quotes":[],"bars":[]}
      {"stream":"listening","data":{"streams":["iex.TSLA"]}}
    We don't validate symbol lists strictly—just confirm we got a subscription/listening notice.
    """
    if isinstance(msg, list):
        return any(_sub_ok(m) for m in msg)
    if not isinstance(msg, dict):
        return False
    if msg.get("T") == "subscription":
        return True
    if msg.get("stream") == "listening":
        return True
    return False

_PARQ_BUFFERS = defaultdict(list)        # per-symbol row buffers
_PARQ_LAST_TS = defaultdict(lambda: 0.0) # per-symbol last flush time (monotonic seconds)
PARQ_FLUSH_SECS = 5.0                    # or 10.0; tune as you like
PARQ_FLUSH_ROWS = 100                    # safety: also flush if we accumulate this many rows

def _get_keys():
    key_id = os.getenv("ALPACA_API_KEY_ID") or os.getenv("APCA_API_KEY_ID")
    secret = os.getenv("ALPACA_API_SECRET") or os.getenv("APCA_API_SECRET_KEY")
    return key_id, secret

# Load environment variables from .env file
load_dotenv()
# WebSocket URLs for Alpaca IEX and SIP feeds
WS_IEX_URL = "wss://stream.data.alpaca.markets/v2/iex"
WS_SIP_URL = "wss://stream.data.alpaca.markets/v2/sip"
STOP = False

def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)

def _backoff():
    delay = 1.0
    while True:
        yield delay
        delay = min(delay * 2, 30.0)

def _flush_row(sym: str, date_str: str, outdir: str, label: str, b: Dict[str, Any], ingress: IngressMode,
               on_bar: Optional[Callable[[Dict[str, Any]], None]]) -> None:
    """
    Persist a completed 1s bucket to disk (if DISK/TEE) and/or call on_bar (if DIRECT/TEE).
    b keys: ts (RFC3339Z), o,h,l,c,v
    """
    row = {
        "timestamp": [b["ts"]],
        "open":      [b["o"]],
        "high":      [b["h"]],
        "low":       [b["l"]],
        "close":     [b["c"]],
        "volume":    [b["v"]],
    }
    df = pd.DataFrame(row)

    if ingress in (IngressMode.DISK, IngressMode.TEE):
        # 1) Always write CSV now
        csv_path = os.path.join(outdir, f"{sym}_{date_str}_{label}_ohlcv-1s.csv")
        write_ohlcv_csv(df, csv_path)
        # 2) Buffer for Parquet
        _PARQ_BUFFERS[sym].append(df)
        now = monotonic()
        should_flush = (now - _PARQ_LAST_TS[sym] >= PARQ_FLUSH_SECS) or (len(_PARQ_BUFFERS[sym]) >= PARQ_FLUSH_ROWS)
        if should_flush:
            batch = pd.concat(_PARQ_BUFFERS[sym], ignore_index=True)
            parquet_sessions.append(batch, outdir, date_str, sym, label)
            _PARQ_BUFFERS[sym].clear()
            _PARQ_LAST_TS[sym] = now

    if ingress in (IngressMode.DIRECT, IngressMode.TEE) and on_bar is not None:
        # convert to a uniform dict for strategies
        on_bar({
            "symbol": sym,
            "ts_utc": pd.to_datetime(b["ts"], utc=True).to_pydatetime(),
            "open": b["o"], "high": b["h"], "low": b["l"], "close": b["c"], "volume": b["v"],
            "source": label
        })

async def stream_live(symbols: List[str], date_str: str, outdir: str, label: str,
                      feed: str, ingress: IngressMode, on_bar: Optional[Callable[[Dict[str, Any]], None]] = None, channel: str = "trades", exit_on_interrupt: bool = True, runner=None):
    """Live trades → 1s bars → disk and/or callback (depending on ingress)."""
    ws_url = WS_SIP_URL if feed.lower() == "sip" else WS_IEX_URL
    bucket: Dict[str, Optional[Dict[str, Any]]] = {}
    last_second: Dict[str, Any] = {}

    key_id, secret = _get_keys()

    interrupted = False
    try:
        for delay in _backoff():
            if STOP: break
            try:
                async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20) as ws:
                    # 1) AUTH
                    await ws.send(json.dumps({"action": "auth", "key": key_id, "secret": secret}))
                    # Some servers send a "connected" banner first; keep reading until we see a real auth ack
                    authed = False
                    last = None
                    for _ in range(20):  # up to ~20 frames; bounded so we can't loop forever
                        try:
                            frame = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        except asyncio.TimeoutError:
                            break
                        try:
                            msg = json.loads(frame)
                        except Exception:
                            continue
                        last = msg
                        if _auth_ok(msg):
                            authed = True
                            break
                        # Ignore banners like [{"T":"success","msg":"connected"}]
                    if not authed:
                        raise RuntimeError(f"WS auth failed (timeout). last={last}")
                    print("[ALPACA] authenticated ✔", flush=True)

                    # 2) SUBSCRIBE (trades or quotes; IEX or SIP)
                    sub = {"action": "subscribe"}
                    if channel == "trades":
                        sub["trades"] = symbols
                    else:
                        sub["quotes"] = symbols
                    await ws.send(json.dumps(sub))
                    sub_ack = json.loads(await ws.recv())
                    if not _sub_ok(sub_ack):
                        print(f"[ALPACA] warning: unexpected subscribe ack: {sub_ack}", flush=True)
                    print(f"[ALPACA] subscribed: {symbols} feed={feed} channel={channel}", flush=True)

                    # 3) MAIN LOOP → aggregate to 1s, write CSV each second, micro-batch Parquet, call on_bar(), and strategy runner if present
                    while not STOP:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        except asyncio.TimeoutError:
                            now_sec = to_utc(pd.Timestamp.utcnow()).floor("1s")
                            for sym, b in list(bucket.items()):
                                if b:
                                    b_ts = to_utc(b["ts"])
                                    if b_ts < now_sec:
                                        _flush_row(sym, date_str, outdir, label, b, ingress, on_bar)
                                        # --- STRATEGY HOOK ---
                                        if runner is not None:
                                            sec_bar = {
                                                "timestamp": to_utc(b["ts"]),
                                                "open": b["o"], "high": b["h"], "low": b["l"], "close": b["c"], "volume": b["v"],
                                                "source": feed.upper(),
                                            }
                                            runner.on_second_bar(sym, sec_bar)
                                        bucket[sym] = None
                            # 👇 Heartbeat: close idle minutes in the runner
                            if runner is not None:
                                runner.on_timer_tick()
                            continue
                        except websockets.ConnectionClosedOK:
                            break
                        except websockets.ConnectionClosedError as e:
                            print("[ALPACA] closed:", e, flush=True); break
                        try:
                            arr = json.loads(raw)
                        except Exception:
                            continue
                        if not isinstance(arr, list):
                            continue
                        for ev in arr:
                            ttype = ev.get("T")
                            if channel == "trades":
                                if ttype != "t":
                                    continue
                                sym = ev.get("S")
                                px  = float(ev.get("p"))
                                sz  = float(ev.get("s", 0))
                                ts  = ev.get("t")
                            else:
                                if ttype != "q":
                                    continue
                                sym = ev.get("S")
                                bp  = float(ev.get("bp", 0))
                                ap  = float(ev.get("ap", 0))
                                if bp <= 0 or ap <= 0:
                                    continue
                                px  = (bp + ap) / 2.0
                                sz  = 0.0
                                ts  = ev.get("t")
                                # --- STRATEGY QUOTE HOOK ---
                                if runner is not None and hasattr(runner, "strategy") and hasattr(runner.strategy, "on_quote"):
                                    q = {
                                        "bid": bp,
                                        "ask": ap,
                                        "bid_size": float(ev.get("bs", 0)),
                                        "ask_size": float(ev.get("as", 0)),
                                        "ts": pd.to_datetime(ts, utc=True),
                                    }
                                    runner.strategy.on_quote(sym, q)
                            ts_ns = to_utc(ts)
                            sec = ts_ns.floor("1s")
                            if last_second.get(sym) != sec:
                                if sym in bucket and bucket[sym]:
                                    _flush_row(sym, date_str, outdir, label, bucket[sym], ingress, on_bar)
                                    # --- STRATEGY HOOK ---
                                    if runner is not None:
                                        b = bucket[sym]
                                        sec_bar = {
                                            "timestamp": to_utc(b["ts"]),
                                            "open": b["o"], "high": b["h"], "low": b["l"], "close": b["c"], "volume": b["v"],
                                            "source": feed.upper(),
                                        }
                                        runner.on_second_bar(sym, sec_bar)
                                bucket[sym] = {
                                    "ts": sec,  # store as tz-aware Timestamp
                                    "o": px, "h": px, "l": px, "c": px, "v": sz
                                }
                                last_second[sym] = sec
                            else:
                                b = bucket.get(sym)
                                if b is None:
                                    bucket[sym] = {
                                        "ts": sec.isoformat().replace("+00:00","Z"),
                                        "o": px, "h": px, "l": px, "c": px, "v": sz
                                    }
                                else:
                                    b["c"] = px
                                    b["h"] = max(b["h"], px)
                                    b["l"] = min(b["l"], px)
                                    b["v"] += sz
                    # after loop: final flush
                    try:
                        for sym in symbols:
                            b = bucket.get(sym)
                            if b:
                                _flush_row(sym, date_str, outdir, label, b, ingress, on_bar)
                                # --- STRATEGY HOOK ---
                                if runner is not None:
                                    sec_bar = {
                                        "timestamp": to_utc(b["ts"]),
                                        "open": b["o"], "high": b["h"], "low": b["l"], "close": b["c"], "volume": b["v"],
                                        "source": feed.upper(),
                                    }
                                    runner.on_second_bar(sym, sec_bar)
                                bucket[sym] = None
                    except Exception as e:
                        print("[ALPACA] flush-on-exit error:", e, flush=True)
            except (asyncio.CancelledError, KeyboardInterrupt):
                interrupted = True
                print("[WS] Ctrl+C → shutting down…", flush=True)
                break
            except Exception as e:
                print("[ALPACA] error:", e, flush=True)
                await asyncio.sleep(delay)
    finally:
        # flush any buffered rows to Parquet and close writers
        close_parquet_sessions()
        if runner is not None:
            runner.stop()
        if exit_on_interrupt and interrupted:
            import sys
            sys.exit(130)


def _et_window(date_str: str):
    et = ZoneInfo("America/New_York")
    day = pd.Timestamp(f"{date_str} 00:00:00", tz=et)
    start_et = day + pd.Timedelta(hours=4)                   # 04:00
    bridge_et = day + pd.Timedelta(hours=9, minutes=15)      # 09:15
    end_et = day + pd.Timedelta(hours=9, minutes=30)         # 09:30 (not used here)
    return start_et, bridge_et, end_et

def _fetch_trades_rest(symbols, start_iso, end_iso, feed, key_id, secret):
    headers = {
        "APCA-API-KEY-ID": key_id,
        "APCA-API-SECRET-KEY": secret,
        "Accept": "application/json",
    }
    params = {
        "symbols": ",".join(symbols),
        "start": start_iso,
        "end": end_iso,
        "limit": 10_000,
        "feed": feed,
    }
    url = "https://data.alpaca.markets/v2/stocks/trades"
    out = []
    next_token = None
    with requests.Session() as s:
        while True:
            p = params.copy()
            if next_token:
                p["page_token"] = next_token
            r = s.get(url, headers=headers, params=p, timeout=30)
            r.raise_for_status()
            data = r.json()
            payload = data.get("trades", {})
            if isinstance(payload, dict):
                for sym, arr in payload.items():
                    for t in arr or []:
                        out.append({"symbol": sym, "ts": t["t"],
                                    "price": float(t["p"]), "size": float(t.get("s", 0))})
            else:
                for t in payload or []:
                    out.append({"symbol": t.get("S"), "ts": t["t"],
                                "price": float(t["p"]), "size": float(t.get("s", 0))})
            next_token = data.get("next_page_token")
            if not next_token:
                break
    return pd.DataFrame(out)

def _trades_to_1s(df_tr: pd.DataFrame) -> pd.DataFrame:
    if df_tr.empty:
        return pd.DataFrame(columns=["timestamp","open","high","low","close","volume","symbol"])
    df = df_tr.copy()
    df["timestamp"] = pd.to_datetime(df["ts"], utc=True)
    df.set_index("timestamp", inplace=True)
    out = []
    for sym, g in df.groupby("symbol"):
        o = g["price"].resample("1s").first()
        h = g["price"].resample("1s").max()
        l = g["price"].resample("1s").min()
        c = g["price"].resample("1s").last()
        v = g["size"].resample("1s").sum().fillna(0)
        bars = pd.concat([o,h,l,c,v], axis=1)
        bars.columns = ["open","high","low","close","volume"]
        bars = bars.dropna(subset=["open"])
        bars["symbol"] = sym
        out.append(bars.reset_index())
    return pd.concat(out, axis=0) if out else pd.DataFrame(columns=["timestamp","open","high","low","close","volume","symbol"])

def warmup_premarket(symbols: List[str], date_str: str, outdir: str, label: str, feed: str, exit_on_interrupt: bool = True):
    """
    Pull SIP REST for premarket from 04:00 ET up to 09:15 ET (or up to 'now-15m' if earlier).
    Writes 1s OHLCV using the same filenames pattern.
    """
    interrupted = False
    try:
        key_id, secret = _get_keys()
        if not key_id or not secret:
            print("[ALPACA] Missing API keys", flush=True); return
        if feed.lower() != "sip":
            print("[ALPACA] warmup uses SIP via REST for consolidated premarket. For free plan IEX REST is not available.", flush=True)

        start_et, bridge_et, _ = _et_window(date_str)
        # honor SIP’s ~15m delay if caller runs this too early
        now_et = pd.Timestamp.now(tz=ZoneInfo("America/New_York"))
        sip_cutoff = min(bridge_et, now_et - pd.Timedelta(minutes=15))

        if sip_cutoff <= start_et:
            print("[ALPACA] Too early for SIP premarket (delay). Try again closer to 09:30.", flush=True)
            return

        start_iso  = start_et.tz_convert("UTC").isoformat().replace("+00:00","Z")
        cutoff_iso = sip_cutoff.tz_convert("UTC").isoformat().replace("+00:00","Z")
        print(f"[ALPACA] SIP REST premarket {date_str} {start_iso} → {cutoff_iso}", flush=True)

        df_tr = _fetch_trades_rest(symbols, start_iso, cutoff_iso, "sip", key_id, secret)
        df_1s = _trades_to_1s(df_tr)

        for sym in symbols:
            df_sym = df_1s[df_1s["symbol"] == sym]
            if df_sym.empty:
                print(f"  [ALPACA] {sym}: no bars produced", flush=True); continue
            df_out = df_sym[["timestamp","open","high","low","close","volume"]]
            save_parquet_append(df_out, outdir, date_str, sym, feed_label=label)
            csv_path = os.path.join(outdir, f"{sym}_{date_str}_{label}_ohlcv-1s.csv")
            write_ohlcv_csv(df_out, csv_path)
            print(f"  [ALPACA] wrote premarket 1s {sym} → {os.path.basename(csv_path)} ({len(df_out)} rows)", flush=True)
    except KeyboardInterrupt:
        interrupted = True
        print("[SIP] Ctrl+C → aborting…", flush=True)
    finally:
        close_parquet_sessions()
        if exit_on_interrupt and interrupted:
            import sys
            sys.exit(130)

def _add_common_args(ap: argparse.ArgumentParser) -> argparse.ArgumentParser:
    ap.add_argument("--symbols", help="Comma separated, e.g. TSLA,MSFT")
    ap.add_argument("--symbols-file", help="Path to a text file with symbols (comma or newline separated)")
    ap.add_argument("--date", required=True, help="YYYY-MM-DD (ET)")
    ap.add_argument("--out", default=default_out("alpaca"), help="Output folder (for DISK/TEE)")
    ap.add_argument("--label", default="IEX", help="Filename label (e.g., IEX or SIP)")
    ap.add_argument("--feed", choices=["iex","sip"], default="iex", help="WebSocket feed to use")
    ap.add_argument("--ingress", choices=[m.value for m in IngressMode], default=IngressMode.DISK.value,
                    help="Where bars go: disk, direct, or tee")
    return ap

def _on_bar_printer(bar: Dict[str, Any]) -> None:
    print(f"[DIRECT] {bar['symbol']} {bar['ts_utc']} o={bar['open']} h={bar['high']} l={bar['low']} c={bar['close']} v={bar['volume']}")

def main():
    global STOP
    top = argparse.ArgumentParser("Alpaca Data CLI")
    sub = top.add_subparsers(dest="cmd", required=True)


    ap_warm = _add_common_args(sub.add_parser("premarket", help="Backfill/warm premarket to disk"))
    ap_live = _add_common_args(sub.add_parser("stream", help="Stream live trades→1s bars"))
    ap_live.add_argument("--channel", choices=["trades","quotes"], default="trades",
                         help="WebSocket channel to stream (trades or quotes)")
    ap_live.add_argument("--strategy", help="Strategy class, e.g. modules.strategy.v13:StrategyV13")
    ap_live.add_argument("--strategy-config", help="Path to JSON/YAML config for the strategy")

    args = top.parse_args()
    symbols = load_symbols(args.symbols, args.symbols_file)
    outdir = ensure_out_dir(args.out)
    ingress = IngressMode(args.ingress)

    for s in (signal.SIGINT, signal.SIGTERM):
        signal.signal(s, lambda *_: globals().__setitem__("STOP", True))

    if args.cmd == "premarket":
        warmup_premarket(symbols, args.date, outdir, args.label, args.feed)
        return

    if args.cmd == "stream":
        # choose optional callback for direct/tee modes
        on_bar = _on_bar_printer if ingress in (IngressMode.DIRECT, IngressMode.TEE) else None

        runner = None
        if args.strategy:
            from modules.strategy.config import load_config
            from modules.strategy.base import StrategyContext
            from modules.backtest.runner import load_strategy
            from modules.stream.runner import StreamStrategyRunner
            strategy_cls = load_strategy(args.strategy)
            strat = strategy_cls(load_config(args.strategy_config))
            ctx = StrategyContext(date=args.date, symbols=symbols, config=load_config(args.strategy_config))
            runner = StreamStrategyRunner(strat, ctx)
            # NEW: warm-start from canonical files
            runner.warm_start_from_canonical(date_str=args.date, symbols=symbols,
                                             minutes=ctx.config.warmup_secs // 60,
                                             canonical_root="data/output", session="extended")
            runner.start()

        asyncio.run(stream_live(symbols, args.date, outdir, args.label, args.feed, ingress, on_bar, channel=args.channel, runner=runner))
        return

if __name__ == "__main__":
    main()

# SIP (REST) = pull historical consolidated data (all US exchanges) over HTTP. On Alpaca’s free plan you can still fetch SIP historical data, but it’s 15-minutes delayed (you can’t ask for the “last 15 minutes” unless you have a SIP subscription). Perfect for pre-market backfill once those minutes are at least 15 minutes old. 
# SIP (WebSocket) = real-time consolidated streaming. This requires a SIP subscription; without it Alpaca returns 409 “insufficient subscription.” That’s the error you hit. 
# IEX = Alpaca’s free real-time stream. Coverage is limited versus full SIP (fewer trades, often quiet after-hours), but it’s great for live testing and RTH strategies. 
# IEX REST = Alpaca does not offer IEX historical data via REST. You can only get historical data from Alpaca if you have a SIP subscription.
#NOTES:
# Data flow (SIP backfill + IEX live bridge)
#
# 1) SIP via REST (premarket backfill): fetch premarket bars up to the cutover
#    time of 09:15:00 ET. Because SIP REST is ~15 minutes delayed, by 09:30 ET
#    we can reliably retrieve data through 09:15 ET.
#
# 2) IEX WebSocket (live): start at 09:15:00 ET and stream live through the
#    rest of the session. This *bridges* the 09:15→close window in real time
#    so there’s no 15-minute hole.
#
# Cutover rule:
#   - All timestamps < 09:15:00 ET → use SIP (REST) data.
#   - All timestamps ≥ 09:15:00 ET → use IEX (WebSocket) data.
#   - If duplicates occur at the boundary (exact 09:15:00), prefer IEX.
#
# Late starts (e.g., launching at 13:00 ET):
#   - Backfill morning data with SIP REST (it will be available up to ~now-15m).
#   - Stream forward with IEX WS for anything from launch time onward.
#
# Implementation notes:
#   - Keep sources labeled (e.g., feed="SIP" vs "IEX") and avoid overwriting IEX
#     with later-arriving SIP for the ≥ 09:15 window.
#   - Make the cutover time configurable in ET in case the provider’s delay changes.
# Manual (exactly your sequence)

# At 09:15 ET:
#
# python -m src.modules.datafeeds.alpaca_data stream \
#   --symbols TSLA,MSFT --date 2025-08-21 --out ./data \
#   --feed iex --ingress tee --label IEX --channel trades
#
# At 09:30 ET:
#
# python -m src.modules.datafeeds.alpaca_data premarket \
#   --symbols TSLA,MSFT --date 2025-08-21 --out ./data \
#   --feed sip --label SIP
#
# At ≈09:45 ET (optional repair once SIP delay has caught up):
#
# python repair_iex_bridge.py --symbols TSLA,MSFT --date 2025-08-21 --out ./data \
# --sip-label SIP --iex-label IEX --output-label MERGED