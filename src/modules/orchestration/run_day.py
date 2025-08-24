# src/modules/orchestration/run_day.py
# run_day.py – The orchestrator of the day:


from __future__ import annotations
import threading
import signal
import sys
import argparse
import json
import os
import subprocess
import time
from dataclasses import dataclass, asdict
from datetime import datetime, date, timedelta, time as dtime
from pathlib import Path
from typing import List, Optional
import pandas as pd
from zoneinfo import ZoneInfo

shutdown = threading.Event()
def _handle_signal(signum, frame):
    print(f"[SHUTDOWN] signal {signum} received", flush=True)
    shutdown.set()
for sig in (signal.SIGINT, signal.SIGTERM):
    try:
        signal.signal(sig, _handle_signal)
    except Exception:
        pass


MODULE_BY_PROVIDER = {
    "alpaca": "alpaca_data",
    "polygon": "polygon_data",
    "databento": "databento_data",
}

ET = ZoneInfo("America/New_York")

STATE_DIR = Path("./logs/state")
LOCK_DIR  = Path("./logs/locks")
STATE_DIR.mkdir(parents=True, exist_ok=True)
LOCK_DIR.mkdir(parents=True, exist_ok=True)

@dataclass
class RunState:
    provider: str
    date: str
    symbols: List[str]
    out: str
    ws_label: str = "IEX"
    sip_label: str = "SIP"
    channel: str = "trades"
    ingress: str = "tee"
    ws_started: bool = False
    sip_done: bool = False
    repair_done: bool = False
    ws_pid: Optional[int] = None
    last_update_et: Optional[str] = None

    @property
    def state_path(self) -> Path:
        return STATE_DIR / f"state_{self.provider}_{self.date}.json"

    @property
    def lock_path(self) -> Path:
        return LOCK_DIR / f"run_{self.provider}_{self.date}.lock"

def et_now() -> datetime:
    return datetime.now(tz=ET)

def et_at(d: date, h: int, m: int, s: int = 0) -> datetime:
    return datetime(d.year, d.month, d.day, h, m, s, tzinfo=ET)

def save_state(st: RunState) -> None:
    st.last_update_et = et_now().isoformat()
    tmp = st.state_path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(asdict(st), indent=2))
    tmp.replace(st.state_path)

def load_state(provider: str, date_str: str) -> Optional[RunState]:
    p = STATE_DIR / f"state_{provider}_{date_str}.json"
    if not p.exists():
        return None
    data = json.loads(p.read_text())
    return RunState(**data)

def acquire_lock(st: RunState) -> None:
    if st.lock_path.exists():
        raise SystemExit(f"[LOCK] Another run appears active: {st.lock_path}")
    st.lock_path.write_text(str(os.getpid()))

def release_lock(st: RunState) -> None:
    try:
        st.lock_path.unlink(missing_ok=True)
    except Exception:
        pass

def build_cmd(provider: str, subcmd: str, symbols_csv: str, date_str: str, out: str,
          label: str, extra: Optional[List[str]] = None) -> List[str]:
    module = MODULE_BY_PROVIDER[provider]
    base = [sys.executable, "-m", f"src.modules.datafeeds.{module}", subcmd,
        "--symbols", symbols_csv, "--date", date_str, "--out", out, "--label", label]
    return base + (extra or [])

def start_ws(st: RunState) -> subprocess.Popen:
    cmd = build_cmd(st.provider, "stream", ",".join(st.symbols), st.date, st.out, st.ws_label,
                    ["--feed", "iex", "--ingress", st.ingress, "--channel", st.channel])
    print("[WS] starting:", " ".join(cmd), flush=True)
    proc = subprocess.Popen(cmd)
    st.ws_started = True
    st.ws_pid = proc.pid  # keep for diagnostics/state file
    save_state(st)
    return proc

def run_sip_premarket(st: RunState) -> int:
    cmd = build_cmd(st.provider, "premarket", ",".join(st.symbols), st.date, st.out, st.sip_label)
    print("[SIP] premarket:", " ".join(cmd), flush=True)
    rc = subprocess.run(cmd).returncode
    if rc == 0:
        st.sip_done = True
        save_state(st)
    else:
        print(f"[SIP] premarket exited with code {rc}", flush=True)
    return rc

# --- Bridge / Repair helpers -------------------------------------------------

COLMAPS = [
    {"ts": "ts", "o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"},
    {"timestamp": "ts", "open": "open", "high": "high", "low": "low", "close": "close", "volume": "volume"},
]

def _find_files(data_dir: Path, symbol: str, date_str: str, label: str) -> List[Path]:
    # Match files like SYMBOL_DATE_LABEL_ohlcv-1s.(parquet|csv)
    patt = f"{symbol}_{date_str}_{label}_ohlcv-1s"
    matches = []
    for p in data_dir.glob("**/*"):
        if p.is_file() and p.suffix.lower() in {".parquet", ".csv"} and patt in p.name:
            matches.append(p)
    # Prefer Parquet first
    matches.sort(key=lambda p: (p.suffix != ".parquet", len(p.name)))
    return matches

def _load_bars(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path)
    else:
        # CSVs were written without headers; try to infer
        try:
            df = pd.read_csv(path, names=["ts","open","high","low","close","volume"])
        except Exception:
            df = pd.read_csv(path)
    cols = {c.lower(): c for c in df.columns}
    canon = None
    for cmap in COLMAPS:
        if all(k in cols for k in cmap.keys()):
            canon = {cols[k]: v for k, v in cmap.items()}
            break
    if canon is None:
        raise ValueError(f"Unrecognized schema for {path} (cols={list(df.columns)})")
    df = df.rename(columns=canon)
    # Ensure datetime tz-aware
    if not pd.api.types.is_datetime64_any_dtype(df["ts"]):
        # try epoch ms fallback then ISO
        df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    if df["ts"].dt.tz is None:
        df["ts"] = df["ts"].dt.tz_localize("UTC")
    df["ts_et"] = df["ts"].dt.tz_convert(ET)
    return df[["ts","ts_et","open","high","low","close","volume"]].sort_values("ts").reset_index(drop=True)

def _sip_has_92959(df_sip: pd.DataFrame) -> bool:
    if df_sip.empty:
        return False
    last_time = df_sip["ts_et"].dt.time.max()
    return last_time >= dtime(9, 29, 59)

def _save_merged(df: pd.DataFrame, out_dir: Path, symbol: str, date_str: str) -> None:
    base = out_dir / f"{symbol}_{date_str}_MERGED_ohlcv-1s"
    df_to_save = df.drop(columns=["ts_et"])
    df_to_save.to_parquet(base.with_suffix(".parquet"), index=False)
    df_to_save.to_csv(base.with_suffix(".csv"), index=False)

def bridge_repair(st: RunState, allow_partial: bool = False) -> None:
    out_dir = Path(st.out)
    t_0930 = dtime(9, 30, 0)

    for sym in st.symbols:
        sip_files = _find_files(out_dir, sym, st.date, st.sip_label)
        iex_files = _find_files(out_dir, sym, st.date, st.ws_label)
        if not sip_files or not iex_files:
            if allow_partial:
                print(f"[BRIDGE] partial merge for {sym} (SIP={bool(sip_files)} IEX={bool(iex_files)})", flush=True)
                # Use whichever side exists
                src = sip_files[0] if sip_files else iex_files[0]
                df = _load_bars(src)
                _save_merged(df, out_dir, sym, st.date)
                continue
            else:
                print(f"[BRIDGE] missing files for {sym}: SIP={bool(sip_files)} IEX={bool(iex_files)}", flush=True)
                continue
        sip = _load_bars(sip_files[0])
        iex = _load_bars(iex_files[0])

        sip_lt_0930 = sip[sip["ts_et"].dt.time < t_0930]
        iex_ge_0930 = iex[iex["ts_et"].dt.time >= t_0930]

        merged = pd.concat([sip_lt_0930, iex_ge_0930], ignore_index=True).sort_values("ts")
        merged = merged.drop_duplicates(subset=["ts"], keep="last").reset_index(drop=True)
        _save_merged(merged, out_dir, sym, st.date)
        print(f"[BRIDGE] {sym}: wrote MERGED ({len(merged)} rows)", flush=True)

    st.repair_done = True
    save_state(st)

# --- Planner / Runner --------------------------------------------------------

def plan_and_run(st: RunState, dry_run: bool = False, wait_for_92959: bool = True, allow_partial_merge: bool = False) -> None:
    trade_date = datetime.strptime(st.date, "%Y-%m-%d").date()
    t_0915 = et_at(trade_date, 9, 15)
    t_0930 = et_at(trade_date, 9, 30)
    t_0945 = et_at(trade_date, 9, 45)

    now = et_now()
    print(f"[INFO] ET now: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}", flush=True)
    print(f"[PLAN] symbols={st.symbols} provider={st.provider} out={st.out}", flush=True)

    if dry_run:
        print(f"  → 09:15 start WS  (label={st.ws_label}, channel={st.channel}, ingress={st.ingress})")
        print(f"  → 09:30 SIP backfill (label={st.sip_label})")
        print(f"  → 09:45 bridge repair (MERGED)", flush=True)
        return

    # Acquire lock early
    acquire_lock(st)
    try:
        # Resume from state if present
        prev = load_state(st.provider, st.date)
        if prev:
            print("[RESUME] Loaded previous state →", prev.__dict__, flush=True)
            st.ws_started = prev.ws_started
            st.sip_done = prev.sip_done
            st.repair_done = prev.repair_done
            st.ws_pid = prev.ws_pid

        # Ensure out dir exists
        Path(st.out).mkdir(parents=True, exist_ok=True)

        # Phase: WS
        ws_proc = None
        if not st.ws_started:
            if now < t_0915:
                while et_now() < t_0915 and not shutdown.is_set():
                    time.sleep(0.5)
                if shutdown.is_set():
                    raise SystemExit(130)
            ws_proc = start_ws(st)
        else:
            print("[WS] already started in previous run", flush=True)

        # Phase: SIP (after 09:30 ET or immediately if past it)
        if not st.sip_done:
            if et_now() < t_0930:
                while et_now() < t_0930 and not shutdown.is_set():
                    time.sleep(0.5)
                if shutdown.is_set():
                    raise SystemExit(130)
            run_sip_premarket(st)
        else:
            print("[SIP] already done", flush=True)

        # Phase: Bridge repair
        if not st.repair_done:
            # If requested, wait until SIP includes 09:29:59 OR until 09:45 ET, whichever first
            if wait_for_92959:
                deadline = max(t_0945, et_now())
                # Poll every 30s until at/after 09:45
                while et_now() < deadline and not shutdown.is_set():
                    # quick check of first symbol
                    sym0 = st.symbols[0]
                    out_dir = Path(st.out)
                    sip_files = _find_files(out_dir, sym0, st.date, st.sip_label)
                    if sip_files:
                        try:
                            sip = _load_bars(sip_files[0])
                            if _sip_has_92959(sip):
                                break
                        except Exception:
                            pass
                    time.sleep(30)
                if shutdown.is_set():
                    raise SystemExit(130)
            bridge_repair(st, allow_partial=allow_partial_merge)
        else:
            print("[BRIDGE] already done", flush=True)

        # Keep attached to WS so Ctrl+C terminates runner and WS
        if st.ws_pid and ws_proc is not None:
            try:
                while ws_proc.poll() is None and not shutdown.is_set():
                    time.sleep(0.5)
            finally:
                if ws_proc.poll() is None:
                    print("[WS] terminating…", flush=True)
                    ws_proc.terminate()
                    try:
                        ws_proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        print("[WS] kill -9", flush=True)
                        ws_proc.kill()

    finally:
        release_lock(st)
        if shutdown.is_set():
            sys.exit(130)

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(prog="run_day", description="Self-driving day runner (WS→SIP→Repair)")
    ap.add_argument("--provider", choices=["alpaca","polygon","databento"], default="alpaca")
    ap.add_argument("--symbols", required=True, help="Comma-separated, e.g. TSLA,MSFT")
    ap.add_argument("--date", help="YYYY-MM-DD (ET). Default: today ET")
    ap.add_argument("--out", default=None, help="Output folder root (default provider-specific)")
    ap.add_argument("--ws-label", default="IEX")
    ap.add_argument("--sip-label", default="SIP")
    ap.add_argument("--channel", choices=["trades","quotes"], default="trades")
    ap.add_argument("--ingress", choices=["disk","direct","tee"], default="tee")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-wait-92959", action="store_true", help="Do not wait for SIP to include 09:29:59")
    ap.add_argument("--allow-partial-merge", action="store_true",
                    help="If SIP or IEX is missing, still write MERGED with the available side")
    return ap.parse_args(argv)

def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    today_et = et_now().date()
    trade_date = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else today_et

    # default out root if not provided
    if args.out:
        out = args.out
    else:
        # mirror provider-specific default used by datafeeds.common.default_out
        root = os.getenv("DATA_OUT_ROOT", "./data/output")
        out = str(Path(root) / args.provider)

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    st = RunState(provider=args.provider, date=trade_date.isoformat(), symbols=symbols,
                  out=out, ws_label=args.ws_label, sip_label=args.sip_label,
                  channel=args.channel, ingress=args.ingress)

    plan_and_run(st, dry_run=args.dry_run, wait_for_92959=not args.no_wait_92959, allow_partial_merge=args.allow_partial_merge)

if __name__ == "__main__":
    main()
# Example usage: Dry run (prints plan only)
# python3 -m src.modules.orchestration.run_day \
#   --provider alpaca --symbols TSLA,MSFT --date 2025-08-21 --dry-run

# Normal run (self-driving)
# python3 -m src.modules.orchestration.run_day \
# --provider alpaca --symbols TSLA,MSFT --date 2025-08-21
