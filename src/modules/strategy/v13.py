from __future__ import annotations
from datetime import datetime, timezone, timedelta
import pandas as pd
import logging
from src.modules.strategy.base import StrategyBase, StrategyContext
from src.modules.strategy.config import StrategyConfig
from src.modules.indicators.indicators import ma_bundle, macd, atr, vwap


from typing import Dict, Any, Iterable, Optional, List, Tuple

import pandas as pd
import numpy as np

import json
import os
import re
import math
from pathlib import Path
from zoneinfo import ZoneInfo
import pytz

from dataclasses import asdict, is_dataclass
from types import SimpleNamespace
from datetime import datetime, timedelta

from src.modules.strategy.base import StrategyBase, StrategyContext
from src.modules.strategy.config import StrategyConfig
from src.modules.indicators.indicators import ma_bundle, macd, atr, vwap
# --- logger shim so self._log can be called or have .info/.debug ---
class _LoggerShim:
    def __init__(self, sink=None):
        # sink should be a callable like print(msg) or logger.info(msg)
        self._sink = sink or print

    def __call__(self, *args, **kwargs):
        # allow self._log("...") style
        msg = " ".join(str(a) for a in args)
        self._sink(msg)

    # allow self._log.info("..."), .debug("..."), etc.
    def info(self, *args, **kwargs):  self(*args, **kwargs)
    def debug(self, *args, **kwargs): self(*args, **kwargs)
    def warning(self, *args, **kwargs): self(*args, **kwargs)
    def error(self, *args, **kwargs): self(*args, **kwargs)

# === add somewhere above your main loop / class or in Strategy class body ===
class HA5m:
    def __init__(self):
        self.bucket_start = None
        self.o = self.h = self.l = self.c = None
        self.prev_ha_open = None
        self.prev_ha_close = None

    def _bucket_for(self, ts):
        # floor to 5m
        minute = (ts.minute // 5) * 5
        return ts.replace(minute=minute, second=0, microsecond=0)

    def update(self, ts, o, h, l, c):
        """
        Feed each 1m bar. When a 5m bar completes (i.e., new bucket begins),
        returns a dict with {'ts','ha_open','ha_close','ha_high','ha_low','dir'}
        else returns None.
        """
        b = self._bucket_for(ts)
        out = None

        if self.bucket_start is None:
            # init first bucket with current bar
            self.bucket_start = b
            self.o, self.h, self.l, self.c = o, h, l, c
            return None

        if b == self.bucket_start:
            # still same 5m bucket -> update
            self.h = max(self.h, h)
            self.l = min(self.l, l)
            self.c = c
            return None

        # a new bucket began -> finalize previous 5m bar
        o5, h5, l5, c5 = self.o, self.h, self.l, self.c
        # compute HA
        if self.prev_ha_open is None:
            ha_open = o5  # seed with real open
            ha_close = (o5 + h5 + l5 + c5) / 4.0
        else:
            ha_open = (self.prev_ha_open + self.prev_ha_close) / 2.0
            ha_close = (o5 + h5 + l5 + c5) / 4.0
        ha_high = max(h5, ha_open, ha_close)
        ha_low  = min(l5, ha_open, ha_close)

        self.prev_ha_open = ha_open
        self.prev_ha_close = ha_close

        # 5m bar is considered "up" if HA close > HA open
        direction = "up" if ha_close > ha_open else "down"

        out = {
            "ts": self.bucket_start + timedelta(minutes=5),  # label at the close
            "ha_open": ha_open,
            "ha_close": ha_close,
            "ha_high": ha_high,
            "ha_low": ha_low,
            "dir": direction,
        }

        # start new bucket with current 1m
        self.bucket_start = b
        self.o, self.h, self.l, self.c = o, h, l, c
        return out


def _fmt_ts(ts):
    # ensures the same ISO format you log now: 2025-07-14T13:33:00+00:00
    try:
        return ts.isoformat()
    except AttributeError:
        return str(ts)

def _is_profit_tighter(side: str, prev_stop: float, new_stop: float, entry: float) -> bool:
    """
    Returns True if new_stop is tighter in the P&L-favorable direction.
    For LONG, tighter means higher; for SHORT, tighter means lower.
    """
    if prev_stop is None:
        return False
    if side.upper() == "BUY_LONG" or side.upper() == "LONG" or side.upper() == "BUY":
        return new_stop > prev_stop
    # SHORT side:
    return new_stop < prev_stop


# ---------- TOP-LEVEL, OUTSIDE THE CLASS ----------
def parse_sig_loglines_to_trades(log_lines):
    """
    Pure function. Parses [SIG] lines -> list of trade dicts.
    Always returns a list (possibly empty).
    """
    

    def parse_line(line: str):
        line = line.strip()
        if not line.startswith("[SIG]"):
            return None, {}
        head, *rest = line.split(" | ")
        parts = head.split()  # "[SIG] BUY" -> ["[SIG]","BUY"]
        sig_type = parts[1] if len(parts) > 1 else None
        kv = {}
        for tok in rest:
            if "=" in tok:
                k, v = tok.split("=", 1)
                kv[k.strip()] = v.strip()
        return sig_type, kv

    def fnum(kv, key, default=None):
        try:
            val = kv.get(key)
            return float(val) if val not in (None, "") else default
        except Exception:
            return default

    def inum(kv, key, default=None):
        try:
            val = kv.get(key)
            return int(float(val)) if val not in (None, "") else default
        except Exception:
            return default

    def safe_round(x):
        try:
            x = float(x)
            return 0.0 if not math.isfinite(x) else round(x, 2)
        except Exception:
            return 0.0

    open_trades = {}
    closed = []

    def close_trade(sym, cause, fill_price):
        t = open_trades.get(sym)
        if not t:
            return
        pnl = 0.0
        if t["side"] == "BUY":
            for q, p in t["partials"]:
                pnl += q * (p - t["entry"])
            pnl += t["remaining"] * (fill_price - t["entry"])
        else:  # SELL_SHORT
            for q, p in t["partials"]:
                pnl += q * (t["entry"] - p)
            pnl += t["remaining"] * (t["entry"] - fill_price)

        rps = t["rps"] if t["rps"] and t["rps"] > 0 else 1e-9
        r_mult = pnl / (rps * max(t["size"], 1))
        exit_causes = []
        if cause:
            exit_causes = [cause if cause.startswith("EXIT_") else f"EXIT_{cause}"]

        closed.append({
            "symbol": t["symbol"],
            "side": t["side"],
            "size": int(t["size"]),
            "partials_qtys": list(t["partials_qtys"]),
            "cum_of_original_ok": (t["remaining"] >= 0),
            "PnL_$": safe_round(pnl),
            "PnL_R": safe_round(r_mult),
            "exit_causes": exit_causes,
        })
        open_trades.pop(sym, None)

    for line in (log_lines or []):
        sig_type, kv = parse_line(line)
        if not sig_type:
            continue

        if sig_type in ("BUY", "SELL_SHORT"):
            sym   = kv.get("sym")
            entry = fnum(kv, "entry")
            size  = inum(kv, "size", 0)
            stop  = fnum(kv, "stop", entry)
            if not sym or entry is None or size is None:
                continue
            rps = abs(entry - stop) if stop is not None else 0.0
            # Store timestamps/counters at open
            open_trades[sym] = {
                "symbol": sym,
                "side": sig_type,
                "size": size,
                "entry": entry,
                "stop": stop,
                "rps": rps,
                "partials": [],        # list[(qty, fill_px)]
                "partials_qtys": [],   # list[int]
                "remaining": size,
                "entered_at": kv.get("ts") or kv.get("timestamp"),
                "partials_done": 0,
                "be_active": False,
                "ratchet_idx": -1,
            }

        elif sig_type == "PARTIAL":
            sym = kv.get("sym")
            if not sym or sym not in open_trades:
                continue
            qty  = inum(kv, "qty", 0)

            # Prefer explicit fill, then close, then entry fallback
            fill = fnum(kv, "fill", None)
            if fill is None:
                fill = fnum(kv, "close", None)
            if fill is None:
                fill = open_trades[sym]["entry"]

            t = open_trades[sym]
            qty = min(max(qty or 0, 0), max(t["remaining"], 0))
            if qty > 0:
                t["partials"].append((qty, fill))
                t["partials_qtys"].append(qty)
                t["remaining"] -= qty
                # Increment partials_done counter
                t["partials_done"] = t.get("partials_done", 0) + 1
                if t["remaining"] == 0:
                    close_trade(sym, cause=None, fill_price=t["entry"])

        elif sig_type.startswith("EXIT_"):
            sym = kv.get("sym")
            if not sym or sym not in open_trades:
                continue

            # Prefer fill, then close, then stop, else entry
            fill_val   = fnum(kv, "fill", None)
            close_fill = fnum(kv, "close", None)
            stop_fill  = fnum(kv, "stop", None)

            if fill_val is not None:
                fill_price = fill_val
            elif close_fill is not None:
                fill_price = close_fill
            elif stop_fill is not None:
                fill_price = stop_fill
            else:
                fill_price = open_trades[sym]["entry"]

            close_trade(sym, cause=sig_type, fill_price=fill_price)

    for sym, t in list(open_trades.items()):
        close_trade(sym, cause=None, fill_price=t["entry"])

    return closed


Bar = Dict[str, Any]

class StrategyV13(StrategyBase):

    import logging

    def _compute_position_size(self, entry: float, stop: float, direction: int) -> float:
        """
        Return number of shares to buy/sell for this entry price, matching the old v13 behavior:
        - Fractional sizing by tier (A: highProbSize, B: lateTrendSize)
        - Dynamic cash if enabled, else EQUITY
        - Min notional guard
        NOTE: We keep it independent of symbol; tier is read from a class attribute if available.
        """
        cfg = getattr(self, "cfg_d", {}) or {}

        # --- sizing params (fall back to old-script defaults) ---
        size_mode      = cfg.get("SIZE_MODE", cfg.get("size_mode", "auto"))
        highProbSize   = float(cfg.get("highProbSize", 0.050))   # 5%
        lowProbSize    = float(cfg.get("lowProbSize",  0.025))   # 2.5%
        lateTrendSize  = float(cfg.get("lateTrendSize", lowProbSize))
        use_dyn_cash   = bool(cfg.get("USE_DYNAMIC_CASH", True))
        equity         = float(cfg.get("EQUITY", 200_000.0))
        min_notional   = float(cfg.get("MIN_TRADE_AMOUNT", 1_000.0))

        # Try to detect the current entry tier that the signal logic computed.
        # If the strategy sets one of these during signal evaluation, we’ll use it.
        tier = getattr(self, "_last_entry_tier", None)
        if tier is None:
            tier = getattr(self, "_last_tier", None)

        # --- choose fraction by mode/tier (old v13 rules) ---
        if size_mode == "auto":
            if tier == "B":
                size_frac = lateTrendSize
            else:
                # default to A if unknown (old script biases to 'A' at open)
                size_frac = highProbSize
        elif size_mode == "high":
            size_frac = highProbSize
        else:  # "low"
            size_frac = lowProbSize

        # --- capital base ---
        base_cap = None
        if use_dyn_cash:
            # prefer a runtime cash field if the runner sets one
            for attr in ("available_cash", "cash", "current_balance"):
                val = getattr(self, attr, None)
                if isinstance(val, (int, float)):
                    base_cap = float(val)
                    break
            if base_cap is None and getattr(self, "ctx", None) is not None:
                base_cap = getattr(self.ctx, "available_cash", None) or getattr(self.ctx, "cash", None)
                if isinstance(base_cap, (int, float)):
                    base_cap = float(base_cap)

        if base_cap is None:
            base_cap = equity  # fallback to fixed equity

        trade_amount = base_cap * size_frac

        # Min-notional guard (only meaningful with dynamic cash)
        if use_dyn_cash and trade_amount < min_notional:
            # 0 shares → caller will treat as "skip entry" or simply not place an order
            return 0.0

        if entry is None or entry <= 0:
            # Defensive: if no valid price, do not size
            return 0.0

        # Old script allowed fractional shares in the sim; keep that behavior.
        shares = trade_amount / float(entry)
        return float(shares)
    # --- time helpers ---
    def _as_utc(self, ts):
        """
        Normalize any timestamp-like value (string, datetime, pandas Timestamp)
        to a tz-aware pandas Timestamp in UTC.
        """
        t = pd.Timestamp(ts) if ts is not None else pd.Timestamp.now(tz="UTC")
        if t.tzinfo is None:
            t = t.tz_localize("UTC")
        else:
            t = t.tz_convert("UTC")
        return t

    def _set_now(self, ts):
        try:
            self._last_ts = pd.Timestamp(ts) if ts is not None else None
        except Exception:
            self._last_ts = None

    def _now_iso(self) -> str:
        """
        Return an ISO timestamp for logging. Prefer the most recent bar timestamp if we have it,
        otherwise fall back to current UTC time.
        """
        for name in ("_last_ts", "last_ts", "_last_bar_ts", "bar_ts", "_ts", "ts"):
            ts = getattr(self, name, None)
            if ts is not None:
                try:
                    return ts.isoformat()
                except Exception:
                    return str(ts)
        return datetime.now(timezone.utc).isoformat()

        if s["pos"] == -1:
            entry = float(s["entry"]); stop = float(s["stop"])

            # HA(5m) exit precedence
            if self._maybe_exit_ha(symbol, s, last):
                out.append({"action": "COVER", "symbol": symbol, "qty": "ALL", "type": "MKT",
                            "ts": ts, "reason": "exit_ha_5m"})
                self._flat(symbol, reason="exit_ha_5m", ts=ts)
                return out

            # Management helpers
            # --- Safer BE/trailing logic for SHORT ---
            R = abs(stop - entry)
            favorable_move = entry - close  # for short, lower is better

            # 1) Move stop to BE only after 0.75R favorable
            if favorable_move >= 0.75 * R:
                proposed = min(stop, entry)  # for short, lower = tighter
                if proposed < stop:
                    stop = self._set_stop("SHORT", symbol, ts, close, proposed, entry, stop, reason="be_0.75R")
                    s["stop"] = stop
                    s["be_active"] = True

            # 2) Trail by ATR only after 1.5R favorable
            if favorable_move >= 1.5 * R:
                proposed = min(stop, close + 1.0 * atrv)
                if proposed < stop:
                    stop = self._set_stop("SHORT", symbol, ts, close, proposed, entry, stop, reason="trail_atr_1.5R")
                    s["stop"] = stop

            # --- Ratchet/trailing stop (SHORT) ---
            self._maybe_ratchet_stop("short", s, close, atrv, getattr(self.cfg, "ratchet_pct_short", 4.0))
            self._apply_ratchet(symbol, s, close, ts)

            # 3) hard stop
            if high >= s["stop"]:
                be_exit = s.get("be_active", False) and abs(s["stop"] - entry) <= max(1e-6, 1e-6 * entry)
                self._flat(symbol, reason="stop_hit", ts=ts)
                out.append({"action":"COVER", "symbol":symbol, "qty":"ALL", "type":"MKT",
                            "ts": ts, "reason":"stop_hit", "stop": s["stop"]})
                self._dbg(symbol, last, tag="EXIT_STOP", extra={"stop": f"{s['stop']:.4f}", "high": f"{high:.4f}"})
                if be_exit and getattr(self.cfg, "allow_reentry_after_be", False):
                    s["allow_reentry_short"] = True
                return out

            # 4) partials   SHORT (cumulative-of-original)
            filled = int(s.get("took_partial", 0))
            moved  = (float(s["entry"]) - low) / float(s["entry"]) if s["entry"] else 0.0
            levels = list(self.cfg_d.get("partial_levels", []))

            while filled < len(levels) and s["size"] > 0:
                lvl = levels[filled]
                if moved < float(lvl["move_pct"]):
                    break

                orig = int(s.get("orig_size", s["size"]))
                exited_so_far = orig - s["size"]
                cum_frac = sum(float(levels[i]["exit_fraction"]) for i in range(filled + 1))
                target_exited = int(round(orig * cum_frac))
                qty = max(0, min(target_exited - exited_so_far, s["size"]))

                if getattr(self, "debug_partials", False):
                    self._log(f"[PARTIAL DEBUG] lvl={filled+1} mv={float(lvl['move_pct']):.4f} "
                              f"orig={orig} sold_so_far={exited_so_far} desired_cum={target_exited} to_sell={qty}")

                if qty <= 0:
                    break

                s["size"] -= qty
                out.append({"action": "BUY_TO_COVER", "symbol": symbol, "qty": qty, "type": "MKT",
                            "ts": ts, "reason": f"partial_{filled+1}_at_{float(lvl['move_pct'])*100:.2f}%"})
                self._dbg(symbol, last, tag="PARTIAL",
                          extra={"level": filled+1, "exit_frac": lvl["exit_fraction"], "qty": qty, "rem": s["size"]})

                filled += 1
                if getattr(self.cfg, "partial_one_per_bar", False):
                    break

            s["took_partial"] = filled
            if s["size"] == 0:
                self._flat(symbol, reason="all_partials_exit", ts=ts)
                return out

            # --- Ratchet/trailing stop (SHORT) after partials ---
            self._maybe_ratchet_stop("short", s, close, atrv, getattr(self.cfg, "ratchet_pct_short", 4.0))

            # 5) BE activation after N partials (legacy logic, can be removed if not needed)
            if s["size"] > 0 and filled >= int(getattr(self.cfg, "be_after_partials", 0)):
                proposed = min(s["stop"], entry)
                # Clamp BE for SHORT: never above entry
                proposed = min(proposed, entry)
                s["stop"]  = self._set_stop("SHORT", symbol, ts, close, proposed, entry, s["stop"], reason="be_after_partials")
                s["be_active"] = True

            # 6) bias exit (cross up)
            bullish_cross = (p_ema9 <= p_ema20) and (ema9 > p_ema20)
            if bullish_cross and getattr(self.cfg, "bias_exit_on_cross", False):
                self._flat(symbol, reason="bias_exit", ts=ts)
                out.append({"action":"COVER", "symbol":symbol, "qty":"ALL", "type":"MKT", "ts": ts, "reason":"bias_exit"})
                self._dbg(symbol, last, tag="EXIT_BIAS")
                return out

            return None

    def _elapsed_minutes(self, s: dict, ts) -> float:
        entered = s.get("entered_at")
        if not entered:
            return 0.0
        return (self._as_utc(ts) - self._as_utc(entered)).total_seconds() / 60.0

    # --- Break-even with min-minutes gate ---
    def _maybe_activate_be(self, symbol: str, s: dict, ts) -> None:
        cfg = getattr(self.cfg, "be", {}) or {}
        min_m = float(cfg.get("min_minutes", 0))
        need_partials = int(getattr(self.cfg, "be_after_partials", 0))
        if s.get("be_active"):
            return
        if s.get("partials_done", 0) < need_partials:
            return
        if self._elapsed_minutes(s, ts) + 1e-9 < min_m:
            return
        s["be_active"] = True
        self._log(f"[INFO] BE Activated | sym={symbol} | ts={pd.Timestamp(ts).isoformat()} | after_partials={s.get('partials_done',0)} | minutes={self._elapsed_minutes(s, ts):.1f}")

    # --- Step-ratchet (entry-anchored) ---
    def _apply_ratchet(self, symbol: str, s: dict, price: float, ts) -> None:
        rcfg = getattr(self.cfg, "ratchet", {}) or {}
        if not rcfg.get("enable", False):
            return
        levels = rcfg.get("levels", [])
        if not levels:
            return
        idx = int(s.get("ratchet_idx", -1))
        move = self._profit_pct(s, price)  # favorable move from entry
        if idx + 1 >= len(levels):
            return
        lv = levels[idx + 1]
        trig = float(lv["trigger_move_pct"])
        cush = float(lv["cushion_pct"])
        if move + 1e-12 < trig:
            return

        entry = float(s["entry"])
        if s["side"] == "BUY":
            new_stop = entry * (1.0 + cush)
            if new_stop > s["stop"]:
                s["stop"] = new_stop
        else:  # SELL_SHORT
            new_stop = entry * (1.0 - cush)
            if new_stop < s["stop"]:
                s["stop"] = new_stop

        s["ratchet_idx"] = idx + 1
        self._log(f"[INFO] RATCHET | sym={symbol} | ts={pd.Timestamp(ts).isoformat()} | stop={float(s['stop']):.4f} | cushion_pct={(cush*100):.2f}")

    # --- HA(5m) exit (simple and robust) ---
    def _init_ha_state(self):
        if not hasattr(self, "_mbar_buf"):
            self._mbar_buf = {}            # sym -> list of minute bars
            self._ha5_prev = {}            # sym -> (ha_open, ha_close)
            self._ha5_color = {}           # sym -> +1 bull, -1 bear, 0 flat
            self._ha5_last_bucket = {}     # sym -> bucket timestamp

    def _ha5m_update(self, symbol: str, bar: pd.Series | dict):
        ecfg = getattr(self.cfg, "ha_exit", {}) or {}
        if not ecfg.get("enable", False):
            return None
        self._init_ha_state()

        tf = int(ecfg.get("timeframe_min", 5))
        ts = self._as_utc(bar["timestamp"])
        bucket = ts.floor(f"{tf}min")

        # stash this minute
        o = float(bar.get("open",  bar.get("close")))
        h = float(bar.get("high",  bar.get("close")))
        l = float(bar.get("low",   bar.get("close")))
        c = float(bar.get("close"))
        self._mbar_buf.setdefault(symbol, []).append({"ts": ts, "o": o, "h": h, "l": l, "c": c})

        last_bucket = self._ha5_last_bucket.get(symbol)
        if last_bucket is None:
            self._ha5_last_bucket[symbol] = bucket
            return None
        if bucket == last_bucket:
            return None

        # compute prior bucket OHLC
        seg = [b for b in self._mbar_buf[symbol] if last_bucket <= b["ts"] < bucket]
        self._ha5_last_bucket[symbol] = bucket
        if not seg:
            return None
        o = seg[0]["o"]; h = max(b["h"] for b in seg); l = min(b["l"] for b in seg); c = seg[-1]["c"]

        # Heiken-Ashi transform
        ha_close = (o + h + l + c) / 4.0
        prev = self._ha5_prev.get(symbol)
        ha_open = (prev[0] + prev[1]) / 2.0 if prev else (o + c) / 2.0
        self._ha5_prev[symbol] = (ha_open, ha_close)

        color = 1 if ha_close > ha_open else -1 if ha_close < ha_open else 0
        prev_color = self._ha5_color.get(symbol, 0)
        self._ha5_color[symbol] = color
        return (prev_color, color)

    def _maybe_exit_ha(self, symbol: str, s: dict, last) -> bool:
        ecfg = getattr(self.cfg, "ha_exit", {}) or {}
        if not ecfg.get("enable", False):
            return False
        minp = float(ecfg.get("min_profit_pct", 0.0))
        if ecfg.get("require_profit", True):
            if self._profit_pct(s, float(last["close"])) + 1e-12 < minp:
                return False

        flip = self._ha5m_update(symbol, last)
        if flip is None:
            return False
        prev_color, color = flip

        # Short: exit on bullish HA; Long: exit on bearish HA
        if s["side"] == "SELL_SHORT" and color == 1:
            self._dbg(symbol, last, tag="EXIT_HA_5M", extra={"fill": f"{float(last['close']):.4f}"})
            return True
        if s["side"] == "BUY" and color == -1:
            self._dbg(symbol, last, tag="EXIT_HA_5M", extra={"fill": f"{float(last['close']):.4f}"})
            return True
        return False



    def _parse_results_from_loglines(self, log_lines):
        # thin wrapper calling the pure top-level function
        return parse_sig_loglines_to_trades(log_lines)

    def _resolve_entry_window(self, session_date) -> Optional[Tuple[datetime, datetime]]:
        """
        Returns (start_utc, end_utc) or None for 'no restriction'.
        Accepts:
          - "HH:MM-HH:MM" (interpreted in self.tz_name)
          - [start, end] same as above
          - {"start": "...","end":"..."} same as above
          - null/None -> no restriction
        """
        spec = self.entry_window_spec
        if not spec:
            return None
        if isinstance(spec, dict):
            spec = f"{spec.get('start','09:30')}-{spec.get('end','16:00')}"
        if isinstance(spec, list) and len(spec) == 2:
            spec = f"{spec[0]}-{spec[1]}"
        if isinstance(spec, str) and "-" in spec:
            s, e = spec.split("-", 1)
            # interpret in local tz, then convert to UTC
            
            local = pytz.timezone(self.tz_name)
            sdt_local = local.localize(datetime.strptime(f"{session_date} {s}", "%Y-%m-%d %H:%M"))
            edt_local = local.localize(datetime.strptime(f"{session_date} {e}", "%Y-%m-%d %H:%M"))
            return (sdt_local.astimezone(pytz.UTC), edt_local.astimezone(pytz.UTC))
        # fallback: disable restriction
        return None
    def _maybe_do_partials(self, ts, price, trade):
        """
        Cumulative-of-original partials.
        For level k with fractions [f0..fk], desired_cum = floor(orig_size * sum_j<=k f_j).
        When level is hit, sell max(0, desired_cum - sold_so_far), clamped by remaining.
        """
        if not self.partial_levels or trade.get("state") != "OPEN":
            return

        side       = trade.get("side", "LONG")
        entry      = float(trade.get("entry_px", price))
        remaining  = int(trade.get("remaining_shares", trade.get("size", 0)))
        sold_so_far= int(trade.get("sold_so_far", 0))

        # Try to recover original size robustly
        orig_size = trade.get("orig_size")
        if orig_size is None:
            # fallback: remaining + sold_so_far or size
            orig_size = remaining + sold_so_far if (remaining or sold_so_far) else int(trade.get("size", 0))
        orig_size = int(orig_size or 0)

        if orig_size <= 0 or remaining <= 0:
            return

        cum_frac = 0.0
        for level_idx, lvl in enumerate(self.partial_levels):
            mv   = float(lvl.get("move_pct", 0.0))
            frac = float(lvl.get("exit_fraction", 0.0))
            if frac <= 0:
                continue

            # Has this level been hit at current price?
            target = entry * (1.0 + mv) if side == "LONG" else entry * (1.0 - mv)
            hit    = price >= target if side == "LONG" else price <= target
            if not hit:
                cum_frac += frac
                continue

            # Desired cumulative and incremental to sell
            cum_frac += frac
            desired_cum    = int(math.floor(orig_size * cum_frac))
            need_to_sell   = max(0, desired_cum - sold_so_far)
            to_sell        = min(remaining, need_to_sell)

            # DEBUG print (never uses unknown 'i'/'to_sell'); recompute locally
            if self.debug_partials:
                logger = getattr(self, "_log", print)
                logger(
                    f"[PARTIAL DEBUG] lvl={level_idx+1} mv={mv:.4f} "
                    f"orig={orig_size} sold_so_far={sold_so_far} "
                    f"desired_cum={desired_cum} to_sell={to_sell}"
                )

            if to_sell <= 0:
                # already satisfied this level; move on
                continue

            fill_px = target  # conservative bookkeeping; replace with your fill logic if different
            self._partial_debug(trade, level_idx)
            self._fill_partial(trade, ts, fill_px, to_sell, level_idx=level_idx)

            # Update local mirrors to keep iterating safely
            sold_so_far += to_sell
            remaining   -= to_sell
            trade["sold_so_far"]      = sold_so_far
            trade["remaining_shares"] = remaining
            trade["size"]             = remaining
            if remaining <= 0:
                break

    def on_start(self, ctx=None, *args, date=None, symbols=None, **kwargs):
        """
        Called by the runner before the backtest/stream begins.
        Make sure we store metadata we need for naming and logs.
        """
        # Accept legacy runner style (positional ctx) or keyword args.
        # Start with kw defaults, then let ctx (if provided) override.
        self.date = date
        self.symbols = list(symbols or [])

        # Normalize ctx from positional or kw
        self.ctx = ctx or kwargs.get("ctx", None)
        if self.ctx is not None:
            self.date = getattr(self.ctx, "date", self.date)
            self.symbols = list(getattr(self.ctx, "symbols", self.symbols))

        # Ensure log buffer exists
        if not hasattr(self, "_log_lines"):
            self._log_lines = []

        # Ensure we have a proper logger and that self._log is callable
        existing_log = getattr(self, "_log", None)

        # If someone injected a Logger into self._log, adopt it
        if isinstance(existing_log, logging.Logger):
            self.logger = existing_log

        # Fallback logger if none present
        if not hasattr(self, "logger") or not isinstance(self.logger, logging.Logger):
            self.logger = logging.getLogger(self.__class__.__name__)

        # If self._log isn't a callable, wrap it so self._log("...") always works
        if not callable(existing_log):
            def _emit(msg: str):
                # buffer the line for later dump (preserves your existing behavior)
                try:
                    self._log_lines.append(str(msg))
                except Exception:
                    pass
                # send to logger; if logger fails, last-resort print
                try:
                    self.logger.info(msg)
                except Exception:
                    print(str(msg), flush=True)
            self._log = _emit

        # Pick up output settings from config (JSON) if present
        self.results_dump_path = self.cfg_d.get("results_dump_path", getattr(self, "results_dump_path", "./logs/results"))
        self.results_run_label = self.cfg_d.get("results_run_label", getattr(self, "results_run_label", "run"))

    # Load tier-specific stop percent mapping if present
    self.fixed_sl_by_tier = dict(self.cfg_d.get("fixed_sl_pct_by_tier", {}))

    # pull from JSON (no hard-coded window here)
    self.entry_window_spec = self.cfg_d.get("entry_window", None)
    self.tz_name = self.cfg_d.get("timezone", "US/Eastern")

    # one-time helpers/containers
    self.ha5m = HA5m()
    self.buffers = {}
    self.state = {}

    # --- entry window UTC bounds (once per run) ---
    self.entry_start_utc, self.entry_end_utc = None, None
    if self.date:
        ew = self._resolve_entry_window(self.date)
        if ew:
            self.entry_start_utc, self.entry_end_utc = ew
            self._log(f"[CONFIG] entry_window={self.entry_window_spec}   "
                    f"{self.entry_start_utc.isoformat()} {self.entry_end_utc.isoformat()}")
        else:
            self._log("[CONFIG] entry_window=None (no restriction)")

    # Initialize buffers/state for the symbols we're going to trade
    for sym in self.symbols:
        self.buffers[sym] = pd.DataFrame(columns=["timestamp","open","high","low","close","volume"])
        self.state[sym] = {
            "pos": 0, "entry": None, "stop": None, "r": None,
            "size": 0, "orig_size": 0, "took_partial": 0, "be_active": False,
            "allow_reentry_long": False, "allow_reentry_short": False,
            "entries_long": 0, "entries_short": 0,
        }
    print(f"[StrategyV13] start date={self.date} symbols={self.symbols}")

    def on_bar(self, symbol: str, bar: Bar) -> Optional[Iterable[Dict[str, Any]]]:

        # --- set current bar time for logging ---
        ts = getattr(bar, "ts", None)
        if ts is None and isinstance(bar, dict):
            ts = bar.get("ts")
        self._set_now(ts)

        # --- prepare state/buffers ---
        df = self.buffers[symbol]
        s  = self.state[symbol]
        out: List[Dict[str, Any]] = []

        # Normalize timestamp to UTC pandas Timestamp
        ts = pd.Timestamp(bar["timestamp"])
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")

        # Append bar to buffer
        row_dict = {
            "timestamp": ts,
            "open":  float(bar.get("open",  bar["close"])),
            "high":  float(bar.get("high",  bar["close"])),
            "low":   float(bar.get("low",   bar["close"])),
            "close": float(bar["close"]),
            "volume": float(bar.get("volume", 0.0)),
        }
        df.loc[len(df)] = row_dict


        # ---- context needed by the logic below ----
        # Use the last row in the DataFrame for all references
        row = df.iloc[-1]
        last = row.to_dict()
        self._last_ts = getattr(bar, "ts", None) or last.get("ts")
        s = self.state[symbol]                          # per-symbol state (dict)
        ts = self._as_utc(row["timestamp"])            # timestamp for this bar (UTC)

        close = row["close"]
        open_ = row["open"]
        high = row["high"]
        low = row["low"]
        # -------------------------------------------
        closes = df["close"]
        ema9_series  = closes.ewm(span=9,  adjust=False).mean()
        ema20_series = closes.ewm(span=20, adjust=False).mean()
        ema9  = float(ema9_series.iloc[-1])
        ema20 = float(ema20_series.iloc[-1])
        p_ema9  = float(ema9_series.iloc[-2])  if len(ema9_series)  > 1 else ema9
        p_ema20 = float(ema20_series.iloc[-2]) if len(ema20_series) > 1 else ema20

        # MACD(12,26,9)
        ema12 = closes.ewm(span=12, adjust=False).mean()
        ema26 = closes.ewm(span=26, adjust=False).mean()
        macd_line = ema12 - ema26
        sig_line  = macd_line.ewm(span=9, adjust=False).mean()
        macd_now = float(macd_line.iloc[-1])
        sig_now  = float(sig_line.iloc[-1])

        # ATR(14)
        hi = df["high"]; lo = df["low"]; cl = df["close"]
        prev_close = cl.shift(1)
        tr = pd.concat([
            (hi - lo).abs(),
            (hi - prev_close).abs(),
            (lo - prev_close).abs()
        ], axis=1).max(axis=1)
        atr_series = tr.ewm(span=14, adjust=False).mean()
        atrv = float(atr_series.iloc[-1])
        fill_px = close  # parity: log minute close as the fill
        # VWAP (session cumulative)
        typical = (hi + lo + cl) / 3.0
        vol = df["volume"].fillna(0.0)
        tpv = (typical * vol).cumsum()
        cv  = vol.cumsum().replace(0, np.nan)
        vwap_series = (tpv / cv).fillna(cl)
        vwap_val = float(vwap_series.iloc[-1])
        above_vwap = close >= vwap_val
        below_vwap = close <  vwap_val

        # Pack 'last' for _dbg / _maybe_exit_ha
        last = {
            "timestamp": ts,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "ema_9": ema9,
            "ema_20": ema20,
            "macd": macd_now,
            "macd_signal": sig_now,
            "vwap": vwap_val,
            "atr": atrv,
        }

        # ---------- entries (only when flat) ----------
        if s["pos"] == 0:
            can_enter = True
            if getattr(self, "entry_start_utc", None) is not None and getattr(self, "entry_end_utc", None) is not None:
                can_enter = (self.entry_start_utc <= ts <= self.entry_end_utc)

            if getattr(self.cfg, "debug_signals", True):
                every = int(self.cfg_d.get("trace_every_n", 1))
                self._trace_i = getattr(self, "_trace_i", 0) + 1
                if every <= 1 or (self._trace_i % every == 0) or can_enter:
                    print(f"[TRACE] gate | ts={ts} | can_enter={can_enter}")

            if can_enter:
                # reset any previous tier flag for this evaluation
                self._last_entry_tier = None
                # LONG signal
                bullish_cross = (p_ema9 <= p_ema20) and (ema9 > ema20)
                long_ok = bullish_cross and (macd_now > sig_now)
                if getattr(self.cfg, "vwap_filter", False):
                    long_ok = long_ok and above_vwap
                can_long = (s["entries_long"] < getattr(self.cfg, "max_trades_per_dir", 1)) or s.get("allow_reentry_long", False)

                # SHORT signal
                allow_shorts = getattr(self.cfg, "allow_shorts", True)
                short_ok = False; can_short = False
                if allow_shorts:
                    bearish_cross = (p_ema9 >= p_ema20) and (ema9 < ema20)
                    short_ok = bearish_cross and (macd_now < sig_now)
                    if getattr(self.cfg, "vwap_filter", False):
                        short_ok = short_ok and below_vwap
                    can_short = (s["entries_short"] < getattr(self.cfg, "max_trades_per_dir", 1)) or s.get("allow_reentry_short", False)

                if getattr(self.cfg, "debug_signals", True):
                    self._log(
                        f"[TRACE] sig | long_ok={long_ok} short_ok={short_ok} "
                        f"vwap_filter={getattr(self.cfg, 'vwap_filter', False)} "
                        f"allow_shorts={allow_shorts} "
                        f"ema9={ema9:.4f} ema20={ema20:.4f} macd={macd_now:.4f} sig={sig_now:.4f} vwap={vwap_val:.4f}"
                    )

                if long_ok and can_long:
                    entry = close
                    stop  = self._initial_stop(entry, atrv, direction=+1)
                    r     = max(1e-9, entry - stop)
                    # Decide sizing tier for this entry
                    self._last_entry_tier = "A"   # cross-based long = Tier A
                    size  = self._compute_position_size(entry=entry, stop=stop, direction=+1)
                    s.update({
                        "pos": +1, "entry": entry, "stop": stop, "r": r,
                        "size": size, "orig_size": size, "took_partial": 0, "be_active": False,
                        "side": "BUY", "symbol": symbol, "best": entry
                    })
                    if s.get("allow_reentry_long"): s["allow_reentry_long"] = False
                    else: s["entries_long"] += 1
                    out.append({"action":"BUY", "symbol":symbol, "qty":size, "type":"MKT",
                                "ts": ts, "entry": entry, "stop": stop,
                                "reason":"ema9>ema20 + macd>sig" + (" + vwap" if getattr(self.cfg, "vwap_filter", False) else "")})
                    self._dbg(symbol, last, tag="BUY", extra={"entry": f"{entry:.4f}", "stop": f"{stop:.4f}", "R": f"{r:.4f}", "size": size})
                    return out

                if short_ok and can_short:
                    entry = close
                    stop  = self._initial_stop(entry, atrv, direction=-1)
                    r     = max(1e-9, stop - entry)
                    # Decide sizing tier for this entry
                    self._last_entry_tier = "A"   # cross-based short = Tier A
                    size  = self._compute_position_size(entry=entry, stop=stop, direction=-1)
                    s.update({
                        "pos": -1, "entry": entry, "stop": stop, "r": r,
                        "size": size, "orig_size": size, "took_partial": 0, "be_active": False,
                        "side": "SELL_SHORT", "symbol": symbol, "best": entry
                    })
                    if s.get("allow_reentry_short"): s["allow_reentry_short"] = False
                    else: s["entries_short"] += 1
                    out.append({"action":"SELL_SHORT", "symbol":symbol, "qty":size, "type":"MKT",
                                "ts": ts, "entry": entry, "stop": stop,
                                "reason":"ema9<ema20 + macd<sig" + (" + vwap" if getattr(self.cfg, "vwap_filter", False) else "")})
                    self._dbg(symbol, last, tag="SELL_SHORT", extra={"entry": f"{entry:.4f}", "stop": f"{stop:.4f}", "R": f"{r:.4f}", "size": size})
                    return out

            return None  # stayed flat

        # ---------- manage LONG ----------
        if s["pos"] == +1:
            entry = float(s["entry"]); stop = float(s["stop"])

            # HA(5m) exit precedence
            if self._maybe_exit_ha(symbol, s, last):
                out.append({"action": "SELL", "symbol": symbol, "qty": "ALL", "type": "MKT",
                            "ts": ts, "reason": "exit_ha_5m"})
                self._flat(symbol, reason="exit_ha_5m", ts=ts)
                return out


            # Management helpers
            self._maybe_activate_be(symbol, s, ts)
            # --- Ratchet/trailing stop (LONG) ---
            self._maybe_ratchet_stop("long", s, close, ts, atrv, getattr(self.cfg, "ratchet_pct_long", 4.0))
            self._apply_ratchet(symbol, s, close, ts)

            # 1) hard stop
            if low <= stop:
                be_exit = s.get("be_active", False) and abs(stop - entry) <= max(1e-6, 1e-6 * entry)
                self._flat(symbol, reason="stop_hit", ts=ts)
                out.append({"action":"SELL", "symbol":symbol, "qty":"ALL", "type":"MKT",
                            "ts": ts, "reason":"stop_hit", "stop": stop})
                self._dbg(symbol, last, tag="EXIT_STOP", extra={"stop": f"{stop:.4f}", "low": f"{low:.4f}"})
                if be_exit and getattr(self.cfg, "allow_reentry_after_be", False):
                    s["allow_reentry_long"] = True
                return out

            # 2) trail stop by ATR (LONG   direction=+1)
            new_stop = self._trail_stop(close, atrv, stop, direction=+1)
            if new_stop > stop:
                if s.get("be_active", False):
                    # Never move stop below the entry price once BE is active
                    new_stop = max(new_stop, float(s["entry"]))
                s["stop"] = self._set_stop("LONG", symbol, ts, close, new_stop, s["entry"], stop, reason="trail_atr")


            # 3) partials (cumulative-of-original)   LONG
            filled = int(s.get("took_partial", 0))
            moved  = (high - float(s["entry"])) / float(s["entry"]) if s["entry"] else 0.0
            levels = list(self.cfg_d.get("partial_levels", []))

            while filled < len(levels) and s["size"] > 0:
                lvl = levels[filled]
                if moved < float(lvl["move_pct"]):
                    break

                orig = int(s.get("orig_size", s["size"]))
                exited_so_far = orig - s["size"]
                cum_frac = sum(float(levels[i]["exit_fraction"]) for i in range(filled + 1))
                target_exited = int(round(orig * cum_frac))
                qty = max(0, min(target_exited - exited_so_far, s["size"]))

                # (optional) debug
                if getattr(self, "debug_partials", False):
                    self._log(f"[PARTIAL DEBUG] lvl={filled+1} mv={float(lvl['move_pct']):.4f} "
                              f"orig={orig} sold_so_far={exited_so_far} desired_cum={target_exited} to_sell={qty}")

                if qty <= 0:
                    break

                s["size"] -= qty
                out.append({"action": "SELL", "symbol": symbol, "qty": qty, "type": "MKT",
                            "ts": ts, "reason": f"partial_{filled+1}_at_{float(lvl['move_pct'])*100:.2f}%"})
                self._dbg(symbol, last, tag="PARTIAL",
                          extra={"level": filled+1, "exit_frac": lvl["exit_fraction"], "qty": qty, "rem": s["size"]})

                filled += 1
                if getattr(self.cfg, "partial_one_per_bar", False):
                    break

            s["took_partial"] = filled
            if s["size"] == 0:
                self._flat(symbol, reason="all_partials_exit", ts=ts)
                return out

            # --- Ratchet/trailing stop (LONG) after partials ---
            self._maybe_ratchet_stop("long", s, close, ts, atrv, getattr(self.cfg, "ratchet_pct_long", 4.0))

            # 4) BE activation after N partials
            if s["size"] > 0 and filled >= int(getattr(self.cfg, "be_after_partials", 0)):
                proposed = max(s["stop"], entry)
                # Clamp BE for LONG: never below entry
                proposed = max(proposed, entry)
                s["stop"]  = self._set_stop("LONG", symbol, ts, close, proposed, entry, s["stop"], reason="be_after_partials")
                s["be_active"] = True

            # 5) bias exit (cross down)
            bearish_cross = (p_ema9 >= p_ema20) and (ema9 < ema20)
            if bearish_cross and getattr(self.cfg, "bias_exit_on_cross", False):
                self._flat(symbol, reason="bias_exit", ts=ts)
                out.append({"action":"SELL", "symbol":symbol, "qty":"ALL", "type":"MKT", "ts": ts, "reason":"bias_exit"})
                self._dbg(symbol, last, tag="EXIT_BIAS")
                return out

            return None

        # ---------- manage SHORT ----------
        if s["pos"] == -1:
            entry = float(s["entry"]); stop = float(s["stop"])

            # HA(5m) exit precedence
            if self._maybe_exit_ha(symbol, s, last):
                out.append({"action": "COVER", "symbol": symbol, "qty": "ALL", "type": "MKT",
                            "ts": ts, "reason": "exit_ha_5m"})
                self._flat(symbol, reason="exit_ha_5m", ts=ts)
                return out


            # Management helpers
            self._maybe_activate_be(symbol, s, ts)
            # --- Ratchet/trailing stop (SHORT) ---
            self._maybe_ratchet_stop("short", s, close, ts, atrv, getattr(self.cfg, "ratchet_pct_short", 4.0))
            self._apply_ratchet(symbol, s, close, ts)

            # 1) hard stop
            if high >= stop:
                be_exit = s.get("be_active", False) and abs(stop - entry) <= max(1e-6, 1e-6 * entry)
                self._flat(symbol, reason="stop_hit", ts=ts)
                out.append({"action":"COVER", "symbol":symbol, "qty":"ALL", "type":"MKT",
                            "ts": ts, "reason":"stop_hit", "stop": stop})
                self._dbg(symbol, last, tag="EXIT_STOP", extra={"stop": f"{stop:.4f}", "high": f"{high:.4f}"})
                if be_exit and getattr(self.cfg, "allow_reentry_after_be", False):
                    s["allow_reentry_short"] = True
                return out

            # 2) trail stop by ATR (SHORT   direction=-1)
            new_stop = self._trail_stop(close, atrv, stop, direction=-1)
            if new_stop < stop:
                if s.get("be_active", False):
                    # Never move stop above the entry price once BE is active (SHORT)
                    new_stop = min(new_stop, float(s["entry"]))
                s["stop"] = self._set_stop("SHORT", symbol, ts, close, new_stop, s["entry"], stop, reason="trail_atr")


            # 3) partials   SHORT (cumulative-of-original)
            filled = int(s.get("took_partial", 0))
            moved  = (float(s["entry"]) - low) / float(s["entry"]) if s["entry"] else 0.0
            levels = list(self.cfg_d.get("partial_levels", []))

            while filled < len(levels) and s["size"] > 0:
                lvl = levels[filled]
                if moved < float(lvl["move_pct"]):
                    break

                orig = int(s.get("orig_size", s["size"]))
                exited_so_far = orig - s["size"]
                cum_frac = sum(float(levels[i]["exit_fraction"]) for i in range(filled + 1))
                target_exited = int(round(orig * cum_frac))
                qty = max(0, min(target_exited - exited_so_far, s["size"]))

                if getattr(self, "debug_partials", False):
                    self._log(f"[PARTIAL DEBUG] lvl={filled+1} mv={float(lvl['move_pct']):.4f} "
                              f"orig={orig} sold_so_far={exited_so_far} desired_cum={target_exited} to_sell={qty}")

                if qty <= 0:
                    break

                s["size"] -= qty
                out.append({"action": "BUY_TO_COVER", "symbol": symbol, "qty": qty, "type": "MKT",
                            "ts": ts, "reason": f"partial_{filled+1}_at_{float(lvl['move_pct'])*100:.2f}%"})
                self._dbg(symbol, last, tag="PARTIAL",
                          extra={"level": filled+1, "exit_frac": lvl["exit_fraction"], "qty": qty, "rem": s["size"]})

                filled += 1
                if getattr(self.cfg, "partial_one_per_bar", False):
                    break

            s["took_partial"] = filled
            if s["size"] == 0:
                self._flat(symbol, reason="all_partials_exit", ts=ts)
                return out

            # --- Ratchet/trailing stop (SHORT) after partials ---
            self._maybe_ratchet_stop("short", s, close, ts, atrv, getattr(self.cfg, "ratchet_pct_short", 4.0))

            # 4) BE activation after N partials
            if s["size"] > 0 and filled >= int(getattr(self.cfg, "be_after_partials", 0)):
                proposed = min(s["stop"], entry)
                # Clamp BE for SHORT: never above entry
                proposed = min(proposed, entry)
                s["stop"]  = self._set_stop("SHORT", symbol, ts, close, proposed, entry, s["stop"], reason="be_after_partials")
                s["be_active"] = True

            # 5) bias exit (cross up)
            bullish_cross = (p_ema9 <= p_ema20) and (ema9 > ema20)
            if bullish_cross and getattr(self.cfg, "bias_exit_on_cross", False):
                self._flat(symbol, reason="bias_exit", ts=ts)
                out.append({"action":"COVER", "symbol":symbol, "qty":"ALL", "type":"MKT", "ts": ts, "reason":"bias_exit"})
                self._dbg(symbol, last, tag="EXIT_BIAS")
                return out

            return None

    ##


    # --- helpers --------------------------------------------------------------

    def _profit_pct(self, s: dict, price: float) -> float:
        """
        Favorable move from entry, signed as a positive percent for the active side.
        LONG:  (price - entry) / entry
        SHORT: (entry - price) / entry
        """
        try:
            entry = float(s.get("entry") or s.get("entry_px") or 0.0)
            if entry <= 0:
                return 0.0
            p = float(price)
            if s.get("side") == "SELL_SHORT" or s.get("pos") == -1:
                return (entry - p) / entry
            return (p - entry) / entry
        except Exception:
            return 0.0

    def _set_stop(
        self,
        side: str,
        symbol: str,
        ts,
        price: float,
        proposed: float,
        entry: float,
        prev_stop: float,
        *,
        reason: str = ""
    ) -> float:
        """
        Clamp stop so it never loosens:
          - LONG:  new_stop = max(prev_stop, proposed); if BE active, never below entry
          - SHORT: new_stop = min(prev_stop, proposed); if BE active, never above entry
        Also prints a small info line when the stop actually tightens.
        """
        try:
            ts_iso = pd.Timestamp(ts).isoformat()
        except Exception:
            ts_iso = str(ts)

        new_stop = float(proposed)
        entry = float(entry)
        prev_stop = float(prev_stop)

        if side.upper().startswith("LONG"):
            # tighten only upward
            new_stop = max(prev_stop, new_stop)
            if self.state.get(symbol, {}).get("be_active", False):
                new_stop = max(new_stop, entry)
            if new_stop > prev_stop + 1e-12:
                self._log(f"[INFO] STOP_UPDATE | sym={symbol} | ts={ts_iso} | from={prev_stop:.4f} -> {new_stop:.4f} | reason={reason}")
            return new_stop

        # SHORT
        new_stop = min(prev_stop, new_stop)
        if self.state.get(symbol, {}).get("be_active", False):
            new_stop = min(new_stop, entry)
        if new_stop < prev_stop - 1e-12:
            self._log(f"[INFO] STOP_UPDATE | sym={symbol} | ts={ts_iso} | from={prev_stop:.4f} -> {new_stop:.4f} | reason={reason}")
        return new_stop

    def _maybe_ratchet_stop(self, side: str, s: dict, price: float, ts, atrv: float, ratchet_pct: float) -> None:
        """
        'Best-price' ratchet independent from ATR trail:
          - LONG: track highest price since entry; stop >= best*(1 - ratchet_pct%)
          - SHORT: track lowest price since entry; stop <= best*(1 + ratchet_pct%)
        Does nothing if inputs are missing or ratchet_pct is falsy.
        """
        try:
            rpct = float(ratchet_pct)
        except Exception:
            rpct = 0.0
        if rpct <= 0:
            return

        side_l = (side or "").lower()
        entry = float(s.get("entry") or 0.0)
        if entry <= 0:
            return

        px = float(price)
        if side_l == "long":
            # update best seen
            s["best"] = max(float(s.get("best", entry)), px)
            target = s["best"] * (1.0 - rpct / 100.0)
            if target > float(s["stop"]):
                s["stop"] = self._set_stop("LONG", s.get("symbol", ""), ts, px, target, entry, s["stop"], reason=f"ratchet_{rpct}%")
        else:
            # short
            s["best"] = min(float(s.get("best", entry)), px)
            target = s["best"] * (1.0 + rpct / 100.0)
            if target < float(s["stop"]):
                s["stop"] = self._set_stop("SHORT", s.get("symbol", ""), ts, px, target, entry, s["stop"], reason=f"ratchet_{rpct}%")

    def _initial_stop(self, entry: float, atrv: float, *, direction: int) -> float:
        stop_mode = getattr(self.cfg, "stop_mode", "fixed")
        cfg = getattr(self, "cfg_d", {}) or {}
        if stop_mode == "fixed":
            pct = cfg.get("fixed_sl_pct", 0.10)
            tier = getattr(self, "_last_entry_tier", None) or getattr(self, "_last_tier", None)
            if tier and tier in getattr(self, "fixed_sl_by_tier", {}):
                pct = float(self.fixed_sl_by_tier[tier])

            if direction > 0:
                stop = entry * (1.0 - pct)
            else:
                stop = entry * (1.0 + pct)
            return stop, pct
        # ATR
        k = float(cfg.get("atr_mult", 2.0))
        if direction > 0:
            return entry - k * atrv, None
        else:
            return entry + k * atrv, None

    def _trail_stop(self, close: float, atrv: float, stop: float, *, direction: int) -> float:
        if self.cfg.stop_mode == "fixed":
            # fixed-% mode doesn't trail; hold initial
            return stop
        k = float(self.cfg.atr_mult)
        if direction > 0:
            return max(stop, close - k * atrv)
        else:
            return min(stop, close + k * atrv)

    def _flat(self, symbol: str, *, reason: str, ts: pd.Timestamp) -> None:
        self.state[symbol].update({
            "pos": 0, "entry": None, "stop": None, "r": None, "size": 0,
            "took_partial": 0, "be_active": False
        })

    def on_end(self, *_, **__):
        """
        End-of-run: parse signals -> closed trades; dump JSON with PnL included.
        """
        

        # Parse signal lines to closed trades (includes PnL_$ and PnL_R)
        try:
            parsed = parse_sig_loglines_to_trades(getattr(self, "_log_lines", []) or [])
        except Exception as e:
            print(f"[RESULTS] parse error (ignored): {e}")
            parsed = []

        # Minimal debug
        sig_lines = sum(1 for l in (getattr(self, "_log_lines", []) or []) if isinstance(l, str) and l.startswith("[SIG]"))
        print(f"[RESULTS] parsed_trades={len(parsed)} sig_lines={sig_lines}")

        # Compose filename
        date = getattr(self, "date", "NA")
        syms = list(getattr(self, "symbols", []) or [])
        sym_label = syms[0] if len(syms) == 1 else "_MULTI"
        run_label = getattr(self, "results_run_label", "risk")
        dump_dir = getattr(self, "results_dump_path", "./logs/results")
        os.makedirs(dump_dir, exist_ok=True)
        fname = f"StrategyV13_{date}_{sym_label}_{run_label}.json"

        # Dump payload DO NOT scrub PnL fields
        payload = {
            "date": date,
            "symbols": syms,
            "risk_sizing": getattr(getattr(self, "config", None), "risk_sizing", None),
            "atr_mult": getattr(getattr(self, "config", None), "atr_mult", None),
            "log": list(getattr(self, "_log_lines", []) or []),
            "results": parsed,
        }
        payload["trades"] = parsed  # alias for jq/tests
        payload["summary"] = {
            "trades": len(parsed),
            "total_PnL_$": float(sum(t.get("PnL_$", 0) or 0 for t in parsed)),
            "avg_PnL_R": (sum((t.get("PnL_R") or 0) for t in parsed) / len(parsed)) if parsed else 0.0,
        }
        with open(os.path.join(dump_dir, fname), "w") as f:
            json.dump(payload, f, indent=2)
        print(f"[RESULTS] dumped to {dump_dir}/{fname}")


    def on_quote(self, symbol: str, quote: dict) -> None:
        pass

    def _dbg(self, symbol: str, last: pd.Series, *, tag: str, extra: dict | None = None) -> None:
        if not getattr(self.cfg, "debug_signals", True):
            return
        def f(x):
            try: return f"{float(x):.4f}"
            except Exception: return str(x)

        # Default fill policy: for exits, use the bar close unless explicitly provided.
        if tag.startswith("EXIT_") and (extra is None or "fill" not in extra):
            extra = dict(extra or {})
            extra["fill"] = f"{float(last['close']):.4f}"

        # Strict parity: emit fill=stop for EXIT_STOP, fill=minute close for PARTIAL/other EXIT_*
        if extra is None:
            extra = {}

        if tag == "PARTIAL":
            # parity: partials use the minute close as the fill
            extra.setdefault("fill", f"{float(last['close']):.4f}")

        elif tag == "EXIT_STOP":
            extra.setdefault("fill", f"{float(last['close']):.4f}")

        elif tag.startswith("EXIT_"):
            # other exits: use minute close
            extra.setdefault("fill", f"{float(last['close']):.4f}")

        parts = [
            f"[SIG] {tag}",
            f"sym={symbol}",
            f"ts={pd.Timestamp(last['timestamp']).isoformat()}",
            f"close={f(last['close'])}",
            f"ema9={f(last.get('ema_9'))}",
            f"ema20={f(last.get('ema_20'))}",
            f"macd={f(last.get('macd'))}",
            f"sig={f(last.get('macd_signal'))}",
            f"vwap={f(last.get('vwap'))}",
            f"atr={f(last.get('atr'))}",
        ]
        if extra:
            parts.extend([f"{k}={v}" for k, v in extra.items()])
        self._log(" | ".join(parts))  # Ensure _dbg uses self._log
