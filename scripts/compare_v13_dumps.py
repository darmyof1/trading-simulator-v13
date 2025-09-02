#!/usr/bin/env python3
import json, sys, math, argparse, csv, re
from pathlib import Path
from collections import defaultdict

ANSI_RE = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")  # strip ANSI if present
def clean_line(s: str) -> str:
    return ANSI_RE.sub("", s).strip()

# ---------- IO ----------
def load_json(path):
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return json.load(f)

def load_text(path):
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return f.read()

def coerce_log_to_lines(log_field):
    if log_field is None:
        return []
    if isinstance(log_field, str):
        return log_field.splitlines()
    if isinstance(log_field, list):
        out = []
        for item in log_field:
            if isinstance(item, str):
                out.append(item)
            elif isinstance(item, dict):
                for k in ("text", "msg", "message", "line"):
                    v = item.get(k)
                    if isinstance(v, str):
                        out.append(v)
                        break
        return out
    return []

# ---------- helpers ----------
def _cum_round(x: float, mode: str) -> int:
    if mode == "floor":
        return math.floor(x)
    if mode == "ceil":
        return math.ceil(x)
    return int(round(x))  # nearest

def parse_kv_tokens_from_any(line: str):
    out = {}
    for seg in [p.strip() for p in line.split("|")]:
        if not seg:
            continue
        if "=" in seg:
            parts = [seg]
        else:
            parts = seg.split()
        for p in parts:
            if "=" in p:
                k, v = p.split("=", 1)
                out[k.strip().lower()] = v.strip()
    return out

def looks_like_entry(line: str) -> bool:
    return "[SIG]" in line and ("BUY" in line or "SELL_SHORT" in line) and "entry=" in line

def looks_like_partial(line: str) -> bool:
    # only gate on SIG + PARTIAL; we'll extract qty/rem via regex inside
    return "[SIG]" in line and "PARTIAL" in line

def looks_like_exit(line: str) -> bool:
    return "[SIG]" in line and line.find("EXIT_") != -1 and "close=" in line

def parse_side_from_entry_line(line: str) -> str | None:
    if "SELL_SHORT" in line:
        return "SELL_SHORT"
    if "BUY" in line:
        return "BUY"
    return None

# ---------- PnL ----------
def per_share_profit(side: str, entry: float, fill: float) -> float:
    if side == "BUY":
        return fill - entry
    if side == "SELL_SHORT":
        return entry - fill
    return 0.0

# ---------- core parser from raw text lines ----------
def parse_trades_from_log_lines(lines, cum_round_mode="nearest"):
    trades = []
    current = {}  # sym -> trade dict or None
    # We keep last-known debug "orig" by level if ever needed (not strictly required once level-1 qty+rem appear)
    pending_debug_by_level = {}

    for raw in lines:
        ln = clean_line(raw)
        if not ln:
            continue

        # Entry
        if looks_like_entry(ln):
            kv = parse_kv_tokens_from_any(ln)
            sym  = kv.get("sym") or kv.get("symbol")
            side = parse_side_from_entry_line(ln)
            entry_px = float(kv.get("entry", kv.get("close", 0.0)) or 0.0)  # prefer explicit entry, else close
            stop_px  = kv.get("stop")
            stop_px  = float(stop_px) if stop_px is not None and stop_px != "" else None

            if not sym or not side or entry_px <= 0:
                continue

            # If an open trade exists, finalize it first
            if sym in current and current[sym]:
                trades.append(current[sym])

            current[sym] = {
                "symbol": sym,
                "side": side,
                "entry_px": entry_px,
                "stop_px": stop_px,
                "R": (abs(entry_px - stop_px) if stop_px is not None else None),
                "size": None,          # will derive at first partial (qty+rem) or from debug
                "partials": [],
                "exits": [],
            }
            continue

        # Partial
        if looks_like_partial(ln):
            kv = parse_kv_tokens_from_any(ln)
            sym  = kv.get("sym") or kv.get("symbol")
            if not sym:
                continue
            lvl  = int(float(kv.get("level", 0) or 0))
            frac = float(kv.get("exit_frac", 0.0) or 0.0)
            qty  = int(float(kv.get("qty", 0) or 0))
            rem  = int(float(kv.get("rem", 0) or 0))
            close_px = float(kv.get("close", 0.0) or 0.0)

            # init trade if not started by entry (rare, but handle defensively)
            if sym not in current or current[sym] is None:
                current[sym] = {
                    "symbol": sym,
                    "side": None,
                    "entry_px": None,
                    "stop_px": None,
                    "R": None,
                    "size": None,
                    "partials": [],
                    "exits": [],
                }

            # size from level-1 qty+rem if unknown
            if current[sym]["size"] is None and lvl == 1:
                current[sym]["size"] = qty + rem

            current[sym]["partials"].append({
                "exit_fraction": frac,
                "qty": qty,
                "fill_px": close_px if close_px > 0 else None,
            })

            # close trade if rem == 0 (sold everything via partials)
            if rem == 0:
                trades.append(current[sym])
                current[sym] = None
            continue

        # Exit
        if looks_like_exit(ln):
            kv = parse_kv_tokens_from_any(ln)
            sym = kv.get("sym") or kv.get("symbol")
            if not sym:
                continue
            close_px = float(kv.get("close", 0.0) or 0.0)
            cause = None
            # Extract EXIT_* token as cause label
            m = re.search(r'EXIT_[A-Z]+', ln)
            if m:
                cause = m.group(0)

            if sym not in current or current[sym] is None:
                # Sometimes an exit can appear w/o a tracked entry/partials; ignore safely
                continue

            current[sym]["exits"].append({"cause": cause, "fill_px": close_px if close_px > 0 else None})
            trades.append(current[sym])
            current[sym] = None
            continue

    # Flush any incomplete-but-useful trade
    for sym, t in list(current.items()):
        if t:
            trades.append(t)
            current[sym] = None

    # Compute cum-of-original check now that trades are built
    out = []
    for t in trades:
        orig = int(t.get("size") or 0)
        fracs = [float(p.get("exit_fraction", 0.0) or 0.0) for p in (t.get("partials") or [])]
        got_qtys = [int(p.get("qty", 0) or 0) for p in (t.get("partials") or [])]

        exp_cums, got_cums = [], []
        s = 0.0
        for f in fracs:
            s += f
            exp_cums.append(_cum_round(orig * s, cum_round_mode))
        cum = 0
        for q in got_qtys:
            cum += q
            got_cums.append(cum)
        cum_ok = (orig > 0 and exp_cums == got_cums)

        # PnL
        entry_px = t.get("entry_px")
        side     = t.get("side")
        R_ps     = t.get("R")
        pnl = None
        if entry_px and orig:
            pnl_val = 0.0
            # partials
            for p in (t.get("partials") or []):
                qty = int(p.get("qty", 0) or 0)
                fp  = p.get("fill_px")
                if qty > 0 and fp:
                    pnl_val += qty * per_share_profit(side, entry_px, float(fp))
            # remaining shares via exit
            sold = sum(int(p.get("qty", 0) or 0) for p in (t.get("partials") or []))
            rem  = max(0, orig - sold)
            exits = t.get("exits") or []
            if rem > 0 and exits:
                fp = exits[-1].get("fill_px")
                if fp:
                    pnl_val += rem * per_share_profit(side, entry_px, float(fp))
            pnl = pnl_val

        pnl_R = (pnl / (orig * R_ps)) if (pnl is not None and R_ps and orig > 0) else None

        out.append({
            "run_file": "-",  # filled by caller
            "symbol": t.get("symbol"),
            "side": side or "?",
            "size": orig or "",
            "partials_qtys": got_qtys,
            "cum_of_original_ok": bool(cum_ok),
            "PnL_$": (round(pnl, 2) if pnl is not None else None),
            "PnL_R": (round(pnl_R, 2) if pnl_R is not None else None),
            "exit_causes": [e.get("cause") for e in (t.get("exits") or [])] or [],
        })
    return out

# ---------- summarizers ----------
def summarize_trades(trades, meta, file_stem, cum_round_mode="nearest"):
    """
    Finalize rows:
      - Default size=4 for exit-only legacy rows (to compute $PnL)
      - cum_of_original_ok = True for trades with zero partials (N/A)
      - Compute per-share PnL_R when size unknown but entry/stop/exit exist
    """
    rows = []
    legacy_hint = ("legacy" in (file_stem or "").lower())

    for t in trades:
        symbol   = t.get("symbol")
        side     = t.get("side") or "?"
        entry_px = t.get("entry_px")
        stop_px  = t.get("stop_px")
        R_ps     = (abs(entry_px - stop_px) if (entry_px and stop_px is not None) else t.get("R"))
        partials = t.get("partials") or []
        exits    = t.get("exits") or []

        # Size: prefer parsed; for legacy exit-only trades, default to 4
        orig = int(t.get("size") or 0)
        if orig == 0 and legacy_hint:
            orig = 4  # legacy default base size for exit-only trades

        # qtys and fractions for cum-of-original
        fracs = [float(p.get("exit_fraction", 0.0) or 0.0) for p in partials]
        got_qtys = [int(p.get("qty", 0) or 0) for p in partials]
        exp_cums, got_cums = [], []
        s = 0.0
        for f in fracs:
            s += f
            exp_cums.append(_cum_round(orig * s, cum_round_mode))
        cum = 0
        for q in got_qtys:
            cum += q
            got_cums.append(cum)

        if len(got_qtys) == 0:
            # No partials => N/A, treat as compliant for the metric
            cum_ok = True
        else:
            cum_ok = (orig > 0 and exp_cums == got_cums)

        # ---- PnL computation ----
        pnl = None
        pnl_R = None

        # If we know entry and have fills, compute $PnL
        if entry_px:
            pnl_val = 0.0
            # partials (if any)
            for p in partials:
                qty = int(p.get("qty", 0) or 0)
                fp  = p.get("fill_px")
                if qty > 0 and fp:
                    # per-share profit
                    if side == "BUY":
                        ps = float(fp) - float(entry_px)
                    else:  # SELL_SHORT
                        ps = float(entry_px) - float(fp)
                    pnl_val += qty * ps
            # final exit for remaining size
            sold = sum(int(p.get("qty", 0) or 0) for p in partials)
            rem  = max(0, orig - sold)
            if rem > 0 and exits:
                last_fp = None
                for e in reversed(exits):
                    if e.get("fill_px") is not None:
                        last_fp = float(e["fill_px"])
                        break
                if last_fp is not None:
                    if side == "BUY":
                        ps = last_fp - float(entry_px)
                    else:
                        ps = float(entry_px) - last_fp
                    pnl_val += rem * ps

            # If we had any dollars at all (or just a valid setup), set pnl
            if (sold > 0 or rem > 0) and (sold + rem) > 0:
                pnl = pnl_val

        # Compute R (per-trade) if possible
        if pnl is not None and R_ps and orig > 0:
            pnl_R = pnl / (orig * float(R_ps))
        else:
            # If size unknown but we do have stop & exit, compute per-share R
            if R_ps and entry_px:
                # find last exit fill
                last_fp = None
                for e in reversed(exits):
                    if e.get("fill_px") is not None:
                        last_fp = float(e["fill_px"])
                        break
                if last_fp is not None:
                    # per-share R multiple
                    if side == "BUY":
                        per_share = (last_fp - float(entry_px)) / float(R_ps)
                    else:
                        per_share = (float(entry_px) - last_fp) / float(R_ps)
                    pnl_R = per_share
                    # leave $PnL blank if size is still unknown (risk runs)

        rows.append({
            "run_file": file_stem,
            "symbol": symbol,
            "side": side,
            "size": orig if orig > 0 else "",
            "partials_qtys": got_qtys,
            "cum_of_original_ok": bool(cum_ok),
            "PnL_$": (round(pnl, 2) if pnl is not None else None),
            "PnL_R": (round(pnl_R, 2) if pnl_R is not None else None),
            "exit_causes": [e.get("cause") for e in exits] or [],
        })
    return rows

def summarize_from_json(data, file_stem, cum_round_mode, debug=False):
    meta = {
        "risk_sizing": data.get("risk_sizing"),
        "atr_mult": data.get("atr_mult"),
        "symbols": data.get("symbols"),
        "date": data.get("date"),
    }

    results = data.get("results", []) or []
    if debug:
        print(f"[DEBUG] {file_stem}: keys={sorted(list(data.keys()))}")
        print(f"[DEBUG] {file_stem}: results_len={len(results)}")

    # Prefer full results if they already contain entry/partials/exits;
    # otherwise fall back to raw log lines if present.
    if results:
        rows = []
        for t in results:
            rows.append({
                "run_file": file_stem,
                "symbol": t.get("symbol"),
                "side": t.get("side") or "?",
                "size": int(t.get("size", 0) or 0),
                # StrategyV13-dumped structure already has these:
                "partials_qtys": t.get("partials_qtys", []),
                "cum_of_original_ok": bool(t.get("cum_of_original_ok", True)),
                "PnL_$": t.get("PnL_$"),
                "PnL_R": t.get("PnL_R"),
                "exit_causes": t.get("exit_causes", []),
            })
        return rows, None

    log_field = data.get("log", None)
    log_lines = coerce_log_to_lines(log_field)
    if debug:
        lt = type(log_field).__name__
        print(f"[DEBUG] {file_stem}: log_type={lt}, log_lines={len(log_lines)}")
        for i, s in enumerate(log_lines[:3]):
            print(f"[DEBUG] {file_stem}: log[{i}]={clean_line(s)[:180]}")
        if debug:
            print(f"[DEBUG] {file_stem}: log_type={lt}, log_lines={len(log_lines)}")
            for i, s in enumerate(log_lines[:6]):
                print(f"[DEBUG] {file_stem}: log[{i}]={clean_line(s)[:180]}")

    if log_lines:
        trades = parse_trades_from_log_lines(log_lines, cum_round_mode=cum_round_mode)
        if trades:
            return summarize_trades(trades, meta, file_stem), None
        return [], f"{file_stem}: no parseable PARTIAL/EXIT/ENTRY lines in JSON log[]"

    return [], f"{file_stem}: results[] empty and no log[] present in JSON"

def summarize_from_log_text(text, file_stem, cum_round_mode, debug=False):
    lines = text.splitlines()
    if debug:
        print(f"[DEBUG] {file_stem}: raw_log_lines={len(lines)}")
        for i, s in enumerate(lines[:6]):  # show more for context
            print(f"[DEBUG] {file_stem}: raw[{i}]={clean_line(s)[:180]}")

    rows_or_trades = parse_trades_from_log_lines(lines, cum_round_mode=cum_round_mode)
    if not rows_or_trades:
        return [], f"{file_stem}: no parseable PARTIAL/EXIT/ENTRY lines in .log text"

    # If parser returned FINAL ROWS (they have 'partials_qtys'), just tag and return.
    if isinstance(rows_or_trades, list) and rows_or_trades and isinstance(rows_or_trades[0], dict) \
       and "partials_qtys" in rows_or_trades[0]:
        for r in rows_or_trades:
            r["run_file"] = file_stem
        return rows_or_trades, None

    # Otherwise, treat as RAW trades and summarize.
    return summarize_trades(rows_or_trades, {}, file_stem, cum_round_mode), None

def process_input(path, cum_round_mode, debug=False):
    p = Path(path)
    file_stem = p.stem
    suffix = p.suffix.lower()

    if suffix == ".json":
        try:
            data = load_json(path)
        except Exception as e:
            return [], f"{file_stem}: JSON load error: {e}"
        return summarize_from_json(data, file_stem, cum_round_mode, debug=debug)

    try:
        text = load_text(path)
    except Exception as e:
        return [], f"{file_stem}: text load error: {e}"
    return summarize_from_log_text(text, file_stem, cum_round_mode, debug=debug)

# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser(description="Compare StrategyV13 results dumps or raw logs.")
    ap.add_argument("paths", nargs="+", help="JSON dumps (.json) and/or raw runtime logs (.log)")
    ap.add_argument("--csv-out", help="Optional: write the per-trade table to this CSV path")
    ap.add_argument("--cum-round", choices=["nearest","floor","ceil"], default="nearest",
                    help="Rounding mode for cumulative-of-original check (default: nearest)")
    ap.add_argument("--debug", action="store_true", help="Verbose diagnostics about what each input contains")
    args = ap.parse_args()

    rows = []
    warnings = []

    for path in args.paths:
        r, warn = process_input(path, args.cum_round, debug=args.debug)
        rows.extend(r)
        if warn:
            warnings.append(warn)

    cols = ["run_file","symbol","side","size","partials_qtys","cum_of_original_ok","PnL_$","PnL_R","exit_causes"]
    print(",".join(cols))
    for r in rows:
        print(",".join([
            str(r.get(c)) if not isinstance(r.get(c), list) else str(r.get(c))
            for c in cols
        ]))

    if args.csv_out:
        with open(args.csv_out, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(cols)
            for r in rows:
                w.writerow([
                    r.get("run_file"),
                    r.get("symbol"),
                    r.get("side"),
                    r.get("size"),
                    str(r.get("partials_qtys")),
                    r.get("cum_of_original_ok"),
                    r.get("PnL_$") if r.get("PnL_$") is not None else "",
                    r.get("PnL_R") if r.get("PnL_R") is not None else "",
                    str(r.get("exit_causes")),
                ])
        print(f"\n[CSV] wrote {args.csv_out}")


    agg = defaultdict(lambda: {
        "PnL_$": 0.0,
        "PnL_R_sum": 0.0,
        "R_count": 0,
        "trades": 0,
        "cum_ok": 0,
        "partial_trades": 0
    })
    for r in rows:
        run = r["run_file"]
        agg[run]["trades"] += 1

        pnl_d = r.get("PnL_$")
        if pnl_d is not None:
            agg[run]["PnL_$"] += float(pnl_d)

        pnl_R = r.get("PnL_R")
        if pnl_R is not None:
            agg[run]["PnL_R_sum"] += float(pnl_R)
            agg[run]["R_count"] += 1

        # Only score compliance on trades that actually had partials
        if (r.get("partials_qtys") or []):
            agg[run]["partial_trades"] += 1
            if r["cum_of_original_ok"]:
                agg[run]["cum_ok"] += 1

    print("\n# Aggregates by run")
    print("run,total_PnL_$,avg_PnL_R,trades,cum_of_original_ok_%")
    for run,a in agg.items():
        avg_R = (a["PnL_R_sum"]/a["R_count"]) if a["R_count"] else 0.0
        ok_pct = (100.0 * a["cum_ok"]/a["partial_trades"]) if a["partial_trades"] else None
        ok_str = f"{round(ok_pct,1)}%" if ok_pct is not None else "N/A"
        print(f"{run},{round(a['PnL_$'],2)},{round(avg_R,2)},{a['trades']},{ok_str}")

    if warnings:
        print("\n# Warnings")
        for w in warnings:
            print(f"- {w}")

if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("Usage: python scripts/compare_v13_dumps.py <dump.json|run.log> ...")
        sys.exit(1)
    main()
