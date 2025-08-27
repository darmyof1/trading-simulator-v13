# --- Append a single canonical minute bar to the daily canonical parquet (safe for low volume) ---
def append_canonical_minute(symbol: str, bar: dict, base="data/output/canonical"):
    from pathlib import Path
    import pandas as pd
    d = pd.DataFrame([{
        "timestamp": pd.Timestamp(bar["timestamp"]).tz_convert("UTC"),
        "open": bar["open"], "high": bar["high"], "low": bar["low"],
        "close": bar["close"], "volume": bar["volume"],
    }])
    date_str = pd.Timestamp(bar["timestamp"]).strftime("%Y-%m-%d")
    outdir = Path(base) / symbol
    outdir.mkdir(parents=True, exist_ok=True)
    path = outdir / f"{symbol}_{date_str}_canonical_ohlcv-1m.parquet"
    # Append: read/append/write (simple & safe; fine for low volume)
    if path.exists():
        old = pd.read_parquet(path)
        d = pd.concat([old, d], ignore_index=True)
        # ensure unique & sorted by timestamp
        d = d.drop_duplicates(subset=["timestamp"]).sort_values("timestamp")
    d.to_parquet(path, index=False)
# src/modules/storage/normalize.py
# any provider file → canonical parquet
from __future__ import annotations
import argparse, re, json
from pathlib import Path
from typing import Dict, Optional, Tuple, List
import pandas as pd

# Canonical schema order
CANON = ["timestamp", "open", "high", "low", "close", "volume"]

# Accept both Alpaca- and Databento-like names
ALPACA_RE = re.compile(
    r'^(?P<sym>[A-Z0-9.\-]+)_(?P<date>\d{4}-\d{2}-\d{2})_(?P<label>[A-Za-z0-9\-]+)_ohlcv-1s\.(?P<ext>csv|parquet)$'
)
DB_RE = re.compile(
    r'^(?P<sym>[A-Z0-9.\-]+)_(?P<date>\d{4}-\d{2}-\d{2})_(?P<dataset>[A-Za-z0-9\-]+)_ohlcv-1s\.(?P<ext>csv|parquet)$'
)

def parse_meta(path: Path) -> Tuple[str, str]:
    """Extract (symbol, date) from filename; raise if unknown shape."""
    name = path.name
    m = ALPACA_RE.match(name) or DB_RE.match(name)
    if not m:
        raise ValueError(f"Unrecognized file name: {name}")
    return m.group("sym"), m.group("date")

def _read_any(path: Path) -> pd.DataFrame:
    """Read provider CSV/Parquet and return canonical columns unsorted (we'll sort later)."""
    if path.suffix == ".parquet":
        df = pd.read_parquet(path)
    else:
        # Detect header
        first = path.open("r", encoding="utf-8").readline().lower()
        has_header = "timestamp" in first and any(k in first for k in ("open", "high", "low", "close"))
        if has_header:
            df = pd.read_csv(path)
        else:
            df = pd.read_csv(path, header=None, names=CANON)

    # Lowercase and keep only the canonical columns
    cols = {c: c.lower() for c in df.columns}
    df = df.rename(columns=cols)
    missing = [c for c in CANON if c not in df.columns]
    if missing:
        raise ValueError(f"{path}: missing columns {missing}")

    # Ensure dtypes; timestamp tz-aware UTC
    df = df[CANON].copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    return df

def to_canonical(df: pd.DataFrame) -> pd.DataFrame:
    """Sort, drop dup timestamps, keep canonical cols only."""
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp")
    df = df.drop_duplicates(subset=["timestamp"], keep="last")
    return df[CANON]

def write_canonical(
    df: pd.DataFrame,
    symbol: str,
    date_str: str,
    out_root: Path,
    also_csv: bool = False,
) -> Path:
    outdir = out_root / "canonical" / symbol
    outdir.mkdir(parents=True, exist_ok=True)
    out = outdir / f"{symbol}_{date_str}_canonical_ohlcv-1s.parquet"
    df.to_parquet(out, index=False)
    if also_csv:
        out_csv = out.with_suffix(".csv")
        # Keep full precision; timestamps remain tz-aware UTC strings
        df.to_csv(out_csv, index=False)
    return out

def compose_day(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """Concatenate multiple same-day inputs (e.g., DB early + IEX later) → de-dup by timestamp."""
    if not dfs:
        return pd.DataFrame(columns=CANON)
    df = pd.concat(dfs, axis=0, ignore_index=True)
    return to_canonical(df)

def normalize_one(path: Path, out_root: Path) -> Path:
    symbol, date_str = parse_meta(path)
    df = to_canonical(_read_any(path))
    return write_canonical(df, symbol, date_str, out_root)

def main():
    ap = argparse.ArgumentParser("normalize")
    ap.add_argument("--in", dest="inputs", nargs="+", required=True, help="Input files (CSV/Parquet)")
    ap.add_argument("--out-root", default="data/output", help="Output root (default: data/output)")
    ap.add_argument("--compose", action="store_true", help="If multiple inputs for the same symbol/date, compose into one file")
    ap.add_argument("--also-csv", action="store_true", help="Also write canonical CSV next to Parquet")
    args = ap.parse_args()

    out_root = Path(args.out_root)
    paths = [Path(p) for p in args.inputs]

    if args.compose:
        # group by (symbol, date)
        groups: Dict[Tuple[str,str], List[Path]] = {}
        for p in paths:
            key = parse_meta(p)
            groups.setdefault(key, []).append(p)
        for (symbol, date_str), files in groups.items():
            dfs = [to_canonical(_read_any(p)) for p in files]
            df = compose_day(dfs)
            out = write_canonical(df, symbol, date_str, out_root, also_csv=args.also_csv)
            print(f"[CANON] wrote {out} ({len(df)} rows)")
    else:
        for p in paths:
            # inline normalize_one so we can pass the flag
            symbol, date_str = parse_meta(p)
            df = to_canonical(_read_any(p))
            out = write_canonical(df, symbol, date_str, out_root, also_csv=args.also_csv)
            print(f"[CANON] wrote {out}")

if __name__ == "__main__":
    main()
