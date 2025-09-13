#src/modules/backtest/runner.py
# file → bars → strategy.on_barfrom __future__ import annotations
import argparse, importlib
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple, Optional
import pandas as pd

from src.modules.strategy.base import StrategyBase, StrategyContext
from src.modules.strategy.config import load_config, StrategyConfig
from src.modules.datafeeds.common import load_symbols  # you already have this helper

CANON = ["timestamp", "open", "high", "low", "close", "volume"]

# ---------- helpers ------------------------------------------------------------

def canonical_path(root: Path, symbol: str, date_str: str) -> Path:
    return root / "canonical" / symbol / f"{symbol}_{date_str}_canonical_ohlcv-1s.parquet"

def read_canonical_1s(p: Path) -> pd.DataFrame:
    df = pd.read_parquet(p)
    # enforce schema/order + tz-aware
    df = df[CANON].copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df

def resample_ohlcv(df_1s: pd.DataFrame, rule: str = "1min") -> pd.DataFrame:
    """
    Resample canonical 1s bars to e.g. 1 minute. Index must be DatetimeIndex (UTC).
    """
    g = df_1s.set_index("timestamp").resample(rule, label="left", closed="left")
    o = g["open"].first()
    h = g["high"].max()
    l = g["low"].min()
    c = g["close"].last()
    v = g["volume"].sum()
    out = pd.concat([o, h, l, c, v], axis=1)
    out.columns = CANON[1:]  # O H L C V
    out.index.name = "timestamp"
    out = out.dropna(subset=["open","high","low","close"])  # drop empty buckets
    out = out.reset_index()
    return out[["timestamp","open","high","low","close","volume"]]

def clip_sessions(df: pd.DataFrame, session: str = "extended") -> pd.DataFrame:
    """If you want RTH-only, clip to 09:30–16:00 America/New_York."""
    if session == "extended":
        return df
    import zoneinfo
    ET = zoneinfo.ZoneInfo("America/New_York")
    ts = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(ET)
    mask = (ts.dt.time >= pd.Timestamp("09:30").time()) & (ts.dt.time <= pd.Timestamp("16:00").time())
    return df.loc[mask].reset_index(drop=True)

def warmup_concat(df_hist: pd.DataFrame, df_run: pd.DataFrame, warmup_rows: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Return (warmup_df, run_df). If df_hist is short, warmup may be < warmup_rows.
    """
    if df_hist is None or df_hist.empty or warmup_rows <= 0:
        return pd.DataFrame(columns=df_run.columns), df_run
    # Only take the tail needed
    w = df_hist.tail(warmup_rows)
    return w, df_run

# --- time helpers -------------------------------------------------------------

def to_utc(ts) -> pd.Timestamp:
    """Coerce any ts (str/np/pd) to tz-aware UTC safely."""
    t = pd.Timestamp(ts)
    if t.tzinfo is None:
        return t.tz_localize("UTC")
    return t.tz_convert("UTC")

# ---------- strategy loader ----------------------------------------------------

def load_strategy(spec: str):
    mod_name, _, cls_name = spec.partition(":")
    try:
        mod = importlib.import_module(mod_name)
    except ModuleNotFoundError:
        mod = importlib.import_module(f"src.{mod_name}")
    return getattr(mod, cls_name)

# ---------- main backtest loop -------------------------------------------------

def run_backtest(
    strategy_cls,
    config: StrategyConfig,
    symbols: List[str],
    dates: List[str],
    canonical_root: Path,
    timeframe: str = "1m",
    warmup_mins: int = 200,
    session: str = "extended",
):
    # Init strategy
    strat: StrategyBase = strategy_cls(config)  # type: ignore
    ctx = StrategyContext(date=",".join(dates), symbols=symbols, config=config, extras={})
    strat.on_start(ctx)

    for date_str in dates:
        # Load per symbol
        per_symbol_frames: Dict[str, pd.DataFrame] = {}
        for sym in symbols:
            p = canonical_path(canonical_root, sym, date_str)
            if not p.exists():
                print(f"[BT] missing canonical file: {p}")
                continue
            df1s = read_canonical_1s(p)
            if timeframe != "1s":
                df = resample_ohlcv(df1s, "1min")
            else:
                df = df1s

            df = clip_sessions(df, session=session)

            # Optionally pull warmup from previous day (simplest version: use same day's early bars)
            warmup_rows = warmup_mins if timeframe == "1m" else warmup_mins * 60

            wdf, run_df = warmup_concat(df.iloc[:-1], df, warmup_rows)  # crude "history" = earlier same-day
            # Seed: feed warmup silently
            for _, row in wdf.iterrows():
                bar = row_to_bar(row)
                strat.on_bar(sym, bar)

            # Avoid double-processing: drop any bars <= last warmup ts
            if not wdf.empty:
                cut_ts = wdf["timestamp"].max()
                run_df = run_df[run_df["timestamp"] > cut_ts]

            per_symbol_frames[sym] = run_df

        # K-way merge by timestamp across symbols (simple concat+sort; fine for skeleton)
        merged = []
        for sym, df in per_symbol_frames.items():
            if df is None or df.empty:
                continue
            tmp = df.copy()
            tmp["__symbol__"] = sym
            merged.append(tmp)
        if not merged:
            continue
        all_rows = pd.concat(merged, axis=0, ignore_index=True)
        all_rows = all_rows.sort_values("timestamp")

        # Replay
        for _, row in all_rows.iterrows():
            sym = row["__symbol__"]
            bar = row_to_bar(row)
            strat.on_bar(sym, bar)

    strat.on_end()

def row_to_bar(row: pd.Series) -> Dict:
    return {
        "timestamp": to_utc(row["timestamp"]),
        "open": float(row["open"]),
        "high": float(row["high"]),
        "low": float(row["low"]),
        "close": float(row["close"]),
        "volume": float(row["volume"]),
        "source": "CANON",
    }

def parse_dates(arg: str) -> List[str]:
    # Accept single YYYY-MM-DD or YYYY-MM-DD:YYYY-MM-DD
    if ":" in arg:
        start, end = arg.split(":", 1)
        dr = pd.date_range(start, end, freq="D")
        return [d.date().isoformat() for d in dr]
    return [arg]

def main():
    ap = argparse.ArgumentParser("backtest-runner")
    ap.add_argument("--symbols", help="Comma-separated symbols")
    ap.add_argument("--symbols-file", help="File with symbols (comma or newline separated)")
    ap.add_argument("--dates", required=True, help="YYYY-MM-DD or start:end")
    ap.add_argument("--canonical-root", default="data/output", help="Root with canonical/<SYM>/*.parquet")
    ap.add_argument("--timeframe", choices=["1s","1m"], default="1m", help="Indicator timeframe")
    ap.add_argument("--warmup-mins", type=int, default=200, help="Warmup length in minutes for 1m (scaled for 1s)")
    ap.add_argument("--sessions", choices=["extended","rth"], default="extended")
    ap.add_argument("--strategy", default="modules.strategy.v13:StrategyV13")
    ap.add_argument("--config", help="Path to JSON/YAML config")
    args = ap.parse_args()

    symbols = load_symbols(args.symbols, args.symbols_file)
    dates = parse_dates(args.dates)
    config = load_config(args.config)
    strategy_cls = load_strategy(args.strategy)

    run_backtest(
        strategy_cls=strategy_cls,
        config=config,
        symbols=symbols,
        dates=dates,
        canonical_root=Path(args.canonical_root),
        timeframe=args.timeframe,
        warmup_mins=args.warmup_mins,
        session=args.sessions,
    )

if __name__ == "__main__":
    main()
