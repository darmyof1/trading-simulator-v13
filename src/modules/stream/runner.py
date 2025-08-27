# src/modules/stream/runner.py
# hooks live on_bar from datafeed → strategy.on_bar

from __future__ import annotations
import pandas as pd
from pathlib import Path
from typing import Dict, Any, Optional, List
import pandas as pd

# Prefer the shared UTC helper; fallback if not present
try:
    from modules.utils.time import to_utc
except Exception:
    def to_utc(ts):
        t = pd.Timestamp(ts)
        return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")

# Try to import your storage helpers; if not available, define small fallbacks
try:
    from modules.storage.normalize import (
        canonical_path, read_canonical_1s, resample_ohlcv, clip_sessions
    )
except Exception:
    def canonical_path(root: Path, symbol: str, date_str: str) -> Path:
        # Matches your warm message path:
        # data/output/canonical/TSLA/TSLA_2025-08-26_canonical_ohlcv-1s.parquet
        return Path(root) / "canonical" / symbol / f"{symbol}_{date_str}_canonical_ohlcv-1s.parquet"

    def read_canonical_1s(path: Path) -> Optional[pd.DataFrame]:
        if not path.exists():
            return None
        df = pd.read_parquet(path)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        return df.sort_values("timestamp").reset_index(drop=True)

    def resample_ohlcv(df_1s: pd.DataFrame, rule: str = "1T") -> pd.DataFrame:
        g = df_1s.set_index("timestamp").resample(rule)
        out = pd.DataFrame({
            "open":  g["open"].first(),
            "high":  g["high"].max(),
            "low":   g["low"].min(),
            "close": g["close"].last(),
            "volume": g["volume"].sum(),
        }).dropna(subset=["open","high","low","close"])
        return out.reset_index()

    def clip_sessions(df_1m: pd.DataFrame, session: str) -> pd.DataFrame:
        # RTH (US equities): 09:30–16:00 America/New_York, else pass-through
        if session.lower() != "rth":
            return df_1m.reset_index(drop=True)
        ny = df_1m["timestamp"].dt.tz_convert("America/New_York")
        mask = (ny.dt.time >= pd.Timestamp("09:30").time()) & (ny.dt.time < pd.Timestamp("16:00").time())
        return df_1m.loc[mask].reset_index(drop=True)


Bar = Dict[str, Any]

# --- safe UTC helper (reuse shared util if you have it) ---
try:
    from modules.utils.time import to_utc  # preferred
except Exception:
    def to_utc(ts):
        t = pd.Timestamp(ts)
        return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")

def _minute_start(ts_utc: pd.Timestamp) -> pd.Timestamp:
    """
    Normalize any timestamp (naive or tz-aware) to UTC and floor to the minute.
    """
    return to_utc(ts_utc).floor("min")


class SecondToMinuteAggregator:
    def __init__(self):
        self.cur: Dict[str, Optional[Dict[str, Any]]] = {}

    def on_second(self, symbol: str, bar: Bar) -> Optional[Bar]:
        ts = pd.Timestamp(bar["timestamp"])
        mstart = _minute_start(ts)
        bucket = self.cur.get(symbol)

        if not bucket:
            self.cur[symbol] = {
                "min_start": mstart,
                "open": float(bar["open"]),
                "high": float(bar["high"]),
                "low":  float(bar["low"]),
                "close": float(bar["close"]),
                "volume": float(bar["volume"]),
                "source": bar.get("source", "LIVE"),
            }
            return None

        if bucket["min_start"] == mstart:
            bucket["high"] = max(bucket["high"], float(bar["high"]))
            bucket["low"]  = min(bucket["low"],  float(bar["low"]))
            bucket["close"] = float(bar["close"])
            bucket["volume"] += float(bar["volume"])
            bucket["source"] = bar.get("source", bucket.get("source", "LIVE"))
            return None

        # minute rolled → emit previous
        finished = {
            "timestamp": bucket["min_start"],
            "open":  bucket["open"],
            "high":  bucket["high"],
            "low":   bucket["low"],
            "close": bucket["close"],
            "volume": bucket["volume"],
            "source": bucket.get("source", "LIVE"),
        }
        # start new bucket
        self.cur[symbol] = {
            "min_start": mstart,
            "open": float(bar["open"]),
            "high": float(bar["high"]),
            "low":  float(bar["low"]),
            "close": float(bar["close"]),
            "volume": float(bar["volume"]),
            "source": bar.get("source", "LIVE"),
        }
        return finished

    def flush_idle(self, now_ts: Optional[pd.Timestamp] = None) -> list[Bar]:
        """
        Close any minute that is strictly before the current minute.
        Works even if now_ts is already tz-aware (UTC) or naive.
        """
        now = to_utc(now_ts or pd.Timestamp.utcnow())
        prev_minute = now.floor("min") - pd.Timedelta(minutes=1)

        out: list[Bar] = []
        for sym, bucket in list(self.cur.items()):
            if not bucket:
                continue
            if bucket["min_start"] <= prev_minute:
                out.append({
                    "timestamp": bucket["min_start"],
                    "open":  bucket["open"],
                    "high":  bucket["high"],
                    "low":   bucket["low"],
                    "close": bucket["close"],
                    "volume": bucket["volume"],
                    "source": bucket.get("source", "LIVE"),
                    "symbol": sym,  # include symbol for convenience
                })
                self.cur[sym] = None
        return out

class StreamStrategyRunner:
    """
    Bridges live per-second bars to a Strategy by aggregating to 1-minute.
    """
    def __init__(self, strategy, ctx):
        self.strategy = strategy
        self.ctx = ctx
        self.agg = SecondToMinuteAggregator()
        self.started = False

    def start(self):
        if not self.started:
            self.strategy.on_start(self.ctx)
            self.started = True

    def stop(self):
        # Close any finished minutes before ending
        for mbar in self.agg.flush_idle():
            sym = mbar.get("symbol") or (self.ctx.symbols[0] if self.ctx.symbols else None)
            if sym is not None:
                self.strategy.on_bar(sym, mbar)
        self.strategy.on_end()
    # NEW: call this from your event loop once per second to close idle minutes
    def on_timer_tick(self, now_ts: Optional[pd.Timestamp] = None):
        for mbar in self.agg.flush_idle(now_ts=now_ts):
            sym = mbar.get("symbol") or (self.ctx.symbols[0] if self.ctx.symbols else None)
            if sym is None:
                continue
            # DEBUG: minute closed by idle flush
            print(f"[MIN*] {sym} {pd.Timestamp(mbar['timestamp']).isoformat()} "
                  f"o={mbar['open']:.2f} h={mbar['high']:.2f} "
                  f"l={mbar['low']:.2f} c={mbar['close']:.2f} v={mbar['volume']:.0f}")
            self.strategy.on_bar(sym, mbar)

    def on_second_bar(self, symbol: str, sec_bar: Bar):
        """
        Feed from your Alpaca stream per-second callback.
        Emits a minute bar to the Strategy when a minute completes.
        """
        mbar = self.agg.on_second(symbol, sec_bar)
        if mbar is not None:
            # DEBUG: minute closed
            print(f"[MIN] {symbol} {pd.Timestamp(mbar['timestamp']).isoformat()} "
                  f"o={mbar['open']:.2f} h={mbar['high']:.2f} "
                  f"l={mbar['low']:.2f} c={mbar['close']:.2f} v={mbar['volume']:.0f}")
            self.strategy.on_bar(symbol, mbar)

    def warm_start_from_canonical(self, date_str: str, symbols: list[str], minutes: int = 200,
                                  canonical_root: str = "data/output", session: str = "extended"):
        """
        Load last `minutes` of minute bars from canonical 1s files and feed on_bar()
        before live streaming starts.
        """
        root = Path(canonical_root)
        seeded_total = 0
        per_sym = {}
        for sym in symbols:
            p = canonical_path(root, sym, date_str)
            if not p.exists():
                print(f"[WARM] no canonical file for {sym} {date_str}: {p}")
                continue
            df1s = read_canonical_1s(p)
            dfm = resample_ohlcv(df1s, "1min")
            dfm = clip_sessions(dfm, session=session)
            if dfm.empty:
                continue
            tail = dfm.tail(minutes)  # last N minutes
            count = 0
            for _, row in tail.iterrows():
                ts = pd.Timestamp(row["timestamp"])
                if ts.tzinfo is None:
                    ts = ts.tz_localize("UTC")
                else:
                    ts = ts.tz_convert("UTC")
                bar = {
                    "timestamp": ts,
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low":  float(row["low"]),
                    "close":float(row["close"]),
                    "volume": float(row["volume"]),
                    "source": "CANON",
                }
                self.strategy.on_bar(sym, bar)
                count += 1
                seeded_total += 1
            per_sym[sym] = count
        if seeded_total > 0:
            details = ", ".join(f"{s}:{n}" for s, n in per_sym.items())
            print(f"[WARM] seeded minute bars from canonical ({details})")
