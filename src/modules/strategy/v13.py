
# ---------- END TOP-LEVEL ----------
from __future__ import annotations
from typing import Dict, Any, Iterable, Optional, List
import pandas as pd
import math
from dataclasses import asdict, is_dataclass
from types import SimpleNamespace
import numpy as np
from zoneinfo import ZoneInfo
from pathlib import Path
import json
import os

# === add near other imports ===
from datetime import datetime, timedelta
from typing import Optional, Tuple

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

from modules.strategy.base import StrategyBase, StrategyContext
from modules.strategy.config import StrategyConfig
from modules.indicators.indicators import ma_bundle, macd, atr, vwap
# ---------- TOP-LEVEL, OUTSIDE THE CLASS ----------
def parse_sig_loglines_to_trades(log_lines):
    """
    Pure function. Parses [SIG] lines -> list of trade dicts.
    Always returns a list (possibly empty).
    """
    import math

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

    def _set_stop(self, side: str, sym: str, ts, close_px: float,
                  new_stop: float, entry_px: float, prev_stop: Optional[float],
                  reason: Optional[str] = None):
        """
        Call this instead of direct assignments to stop_long/stop_short.
        It logs [INFO] BE when new_stop == entry_px and it tightened vs previous,
        otherwise [INFO] RATCHET when it tightened in profit direction.
        Returns new_stop (so you can assign).
        """
        event = None
        if new_stop is None:
            return new_stop

        # breakeven?
        if abs(new_stop - entry_px) < 1e-10 and _is_profit_tighter(side, prev_stop, new_stop, entry_px):
            event = "BE"
        elif _is_profit_tighter(side, prev_stop, new_stop, entry_px):
            event = "RATCHET"

        if event:
            msg = (f"[INFO] {event} | sym={sym} | ts={_fmt_ts(ts)} | "
                   f"prev={prev_stop:.4f} | new={new_stop:.4f} | fill={close_px:.4f}")
            if reason:
                msg += f" | why={reason}"
            self._log(msg)  # use your same append path as [SIG]

        return new_stop
    # --- time helpers ---
    def _as_utc(self, ts):
        t = pd.Timestamp(ts)
        if t.tzinfo is None:
            t = t.tz_localize("UTC")
        else:
            t = t.tz_convert("UTC")
        return t

    def _in_entry_window(self, ts) -> bool:
        ew = getattr(self.cfg, "entry_window", None) or {}
        if not ew or not ew.get("enable", False):
            return True
        tz = ZoneInfo(ew.get("timezone", "America/New_York"))
        t = self._as_utc(ts).tz_convert(tz)
        start = pd.to_datetime(ew.get("start", "09:30")).time()
        end   = pd.to_datetime(ew.get("end",   "10:00")).time()
        return start <= t.time() <= end

    # --- PnL / elapsed helpers ---
    def _profit_pct(self, s: dict, price: float) -> float:
        if s["side"] == "BUY":
            return (price / s["entry"]) - 1.0
        else:
            return (s["entry"] / price) - 1.0

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
            self._dbg(symbol, last, tag="EXIT_HA", extra={"tf": "5m", "fill": f"{float(last['close']):.4f}"})
            self._exit_all(symbol, fill=float(last["close"]), cause="EXIT_HA")
            return True
        if s["side"] == "BUY" and color == -1:
            self._dbg(symbol, last, tag="EXIT_HA", extra={"tf": "5m", "fill": f"{float(last['close']):.4f}"})
            self._exit_all(symbol, fill=float(last["close"]), cause="EXIT_HA")
            return True
        return False

    import re, json, os
    from pathlib import Path

    def _parse_results_from_loglines(self, log_lines):
        # thin wrapper calling the pure top-level function
        return parse_sig_loglines_to_trades(log_lines)


    def _log(self, msg: str) -> None:
        """Store log lines and print them."""
        if not hasattr(self, "_log_lines"):
            self._log_lines = []
        self._log_lines.append(msg)
        print(msg, flush=True)

    def _partial_debug(self, trade, level_idx: int):
        """Print cumulative-of-original math for the given partial level."""
        if not getattr(self, "debug_partials", False):
            return
        try:
            levels = getattr(self, "partial_levels", []) or self.cfg_d.get("partial_levels", [])
            if level_idx < 0 or level_idx >= len(levels):
                return

            mv = float(levels[level_idx].get("move_pct", 0.0))

            # Sum exit fractions up to this level
            frac_sum = 0.0
            for j in range(level_idx + 1):
                frac_sum += float(levels[j].get("exit_fraction", 0.0))

            # Original size (robust recovery)
            orig = int(
                trade.get("orig_size")
                or trade.get("initial_size")
                or trade.get("size_initial", 0)
                or trade.get("size", 0)
                or (trade.get("remaining_shares", 0) + trade.get("sold_so_far", 0))
            )

            rem = int(trade.get("remaining_shares", trade.get("size", 0)))
            sold_so_far = max(0, orig - rem)
            desired_cum = int(math.floor(orig * frac_sum))
            to_sell_dbg = max(0, desired_cum - sold_so_far)

            logger = getattr(self, "_log", print)
            logger(
                f"[PARTIAL DEBUG] lvl={level_idx+1} mv={mv:.4f} "
                f"orig={orig} sold_so_far={sold_so_far} "
                f"desired_cum={desired_cum} to_sell={to_sell_dbg}"
            )
        except Exception:
            # never let debug printing crash the strategy
            pass

    def _compute_position_size(self, *, entry: float, stop: float, direction: int) -> int:
        """
        Returns the position size.
        - If cfg.use_risk_sizing is False: return cfg.base_size (>=1).
        - If True: floor( (balance * risk_pct) / R_per_share ), then clamp and lot-round.
        """
        # default behavior
        if not getattr(self.cfg, "use_risk_sizing", False):
            return max(1, int(getattr(self.cfg, "base_size", 1)))

        bal = float(getattr(self.cfg, "account_balance", 0.0))
        risk_pct = float(getattr(self.cfg, "risk_pct_of_balance", 0.0))
        lot = max(1, int(getattr(self.cfg, "lot_size", 1)))
        smin = max(1, int(getattr(self.cfg, "min_size", 1)))
        smax = max(smin, int(getattr(self.cfg, "max_size", 1)))

        # risk per share (R)
        if direction > 0:
            r_ps = max(1e-6, entry - stop)
        else:
            r_ps = max(1e-6, stop - entry)

        risk_dollars = max(0.0, bal * risk_pct)
        raw = int(risk_dollars // r_ps)  # floor

        # clamp & lot-round
        size = max(smin, min(raw, smax))
        size = (size // lot) * lot
        size = max(smin, min(size, smax))
        return size
    """
    Minute-bar strategy:
      • EMA9/EMA20 cross + MACD confirm
      • Optional VWAP filter
      • ATR or fixed-% initial stop, ATR trail
      • Partials + Breakeven + bias exits
      • Symmetrical SHORT logic (optional)
      • 1 trade per direction per day, with one re-entry after BE
    """

    def __init__(self, config):
        # Accept dict OR dataclass OR a simple object with attrs
        if isinstance(config, dict):
            cfg_d = config
        elif is_dataclass(config):
            cfg_d = asdict(config)
        else:
            try:
                cfg_d = {k: getattr(config, k) for k in vars(config)}
            except Exception:
                cfg_d = {}

        # Keep both a dict and an attribute-style view
        self.cfg_d = cfg_d                      # for dict-style .get()
        self.cfg   = SimpleNamespace(**cfg_d)   # for attribute-style .foo

        # Debug: reuse your existing flag (no JSON changes needed)
        self.debug_partials = bool(self.cfg_d.get("debug_signals", False))

        # Cache partial levels list from config (flat schema)
        self.partial_levels = list(self.cfg_d.get("partial_levels", []))

        # --- entry window resolver config ---
        self.entry_window_spec = self.cfg_d.get("entry_window", "09:30-16:00")  # old engine default RTH
        self.tz_name = self.cfg_d.get("timezone", "US/Eastern")

        # --- HA(5m) aggregator ---
        self.ha5m = HA5m()

        self.ctx: Optional[StrategyContext] = None
        self.buffers: Dict[str, pd.DataFrame] = {}
        # per-symbol position & accounting
        self.state: Dict[str, Dict[str, Any]] = {}

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
            import pytz
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

        # Optional: pick up output settings if not already set
        if not hasattr(self, "results_dump_path"):
            self.results_dump_path = "./logs/results"
        if not hasattr(self, "results_run_label"):
            self.results_run_label = "risk"

        # --- entry window UTC bounds (once per run) ---
        self.entry_start_utc, self.entry_end_utc = None, None
        if self.date:
            ew = self._resolve_entry_window(self.date)
            if ew:
                self.entry_start_utc, self.entry_end_utc = ew
                self._log(f"[CONFIG] entry_window={self.entry_window_spec}  {_fmt_ts(self.entry_start_utc)}–{_fmt_ts(self.entry_end_utc)}")
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
        df = self.buffers[symbol]

        # --- normalize ts to UTC ---
        ts = pd.Timestamp(bar["timestamp"])
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")

        # append minute bar
        df.loc[len(df)] = {
            "timestamp": ts,
            "open": float(bar["open"]),
            "high": float(bar["high"]),
            "low":  float(bar["low"]),
            "close": float(bar["close"]),
            "volume": float(bar["volume"]),
        }

        # indicators
        bundle  = ma_bundle(df)              # ema/sma 9/20/50/200...
        macd_df = macd(df["close"])
        atr_s   = atr(df["high"], df["low"], df["close"]).rename("atr")
        vwap_s  = vwap(df).rename("vwap")

        X = pd.concat([df, bundle, macd_df, atr_s.to_frame(), vwap_s.to_frame()], axis=1)
        X = X.loc[:, ~X.columns.duplicated(keep="last")]
        if len(X) > 2000:
            X = X.iloc[-2000:].reset_index(drop=True)
        self.buffers[symbol] = X

        if len(X) < 2:
            return None
        last = X.iloc[-1]
        prev = X.iloc[-2]

        # warmup (EMA200 optional by default; set require_ema200 if you add it to config later)
        require_ema200 = getattr(self.cfg, "require_ema200", False)
        base_required = ("atr","macd","macd_signal","vwap")
        required = base_required + (("ema_200",) if require_ema200 else ())
        if not all(k in last.index and pd.notna(last[k]) for k in required):
            return None

        # convenience
        close = float(last["close"])
        low   = float(last["low"])
        high  = float(last["high"])
        atrv  = float(last["atr"])
        ema9, ema20 = float(last.get("ema_9", np.nan)), float(last.get("ema_20", np.nan))
        p_ema9, p_ema20 = float(prev.get("ema_9", np.nan)), float(prev.get("ema_20", np.nan))
        macd_now, sig_now = float(last["macd"]), float(last["macd_signal"])
        above_vwap = close >= float(last["vwap"])
        below_vwap = not above_vwap

        s = self.state[symbol]
        out: List[Dict[str, Any]] = []

        # ---------- entries ----------
        if s["pos"] == 0:
            can_enter = True
            if getattr(self, "entry_start_utc", None) is not None and getattr(self, "entry_end_utc", None) is not None:
                can_enter = (self.entry_start_utc <= ts <= self.entry_end_utc)

            if can_enter:
                # LONG setup
                bullish_cross = (p_ema9 <= p_ema20) and (ema9 > ema20)
                long_ok = bullish_cross and (macd_now > sig_now)
                if self.cfg.vwap_filter:
                    long_ok = long_ok and above_vwap
                # enforce trade-frequency limits
                can_long = (s["entries_long"] < self.cfg.max_trades_per_dir) or s["allow_reentry_long"]

                if long_ok and can_long:
                    entry = close
                    stop  = self._initial_stop(entry, atrv, direction=+1)
                    r     = max(1e-9, entry - stop)
                    size  = self._compute_position_size(entry=entry, stop=stop, direction=+1)
                    s.update({
                        "pos": +1, "entry": entry, "stop": stop, "r": r,
                        "size": size, "orig_size": size,
                        "took_partial": 0, "be_active": False
                    })
                    # consume reentry flag if used
                    if s["allow_reentry_long"]:
                        s["allow_reentry_long"] = False
                    else:
                        s["entries_long"] += 1
                    out.append({"action":"BUY", "symbol":symbol, "qty":size, "type":"MKT",
                                "ts": ts, "entry": entry, "stop": stop, "reason":"ema9>ema20 + macd>sig" + (" + vwap" if self.cfg.vwap_filter else "")})
                    self._dbg(symbol, last, tag="BUY", extra={"entry": f"{entry:.4f}", "stop": f"{stop:.4f}", "R": f"{r:.4f}", "size": size})
                    return out

                # SHORT setup (optional)
                if self.cfg.allow_shorts:
                    bearish_cross = (p_ema9 >= p_ema20) and (ema9 < ema20)
                    short_ok = bearish_cross and (macd_now < sig_now)
                    if self.cfg.vwap_filter:
                        short_ok = short_ok and below_vwap
                    can_short = (s["entries_short"] < self.cfg.max_trades_per_dir) or s["allow_reentry_short"]

                    if short_ok and can_short:
                        entry = close
                        stop  = self._initial_stop(entry, atrv, direction=-1)
                        r     = max(1e-9, stop - entry)
                        size  = self._compute_position_size(entry=entry, stop=stop, direction=-1)
                        s.update({
                            "pos": -1, "entry": entry, "stop": stop, "r": r,
                            "size": size, "orig_size": size,
                            "took_partial": 0, "be_active": False
                        })
                        if s["allow_reentry_short"]:
                            s["allow_reentry_short"] = False
                        else:
                            s["entries_short"] += 1
                        out.append({"action":"SELL_SHORT", "symbol":symbol, "qty":size, "type":"MKT",
                                    "ts": ts, "entry": entry, "stop": stop, "reason":"ema9<ema20 + macd<sig" + (" + vwap" if self.cfg.vwap_filter else "")})
                        self._dbg(symbol, last, tag="SELL_SHORT", extra={"entry": f"{entry:.4f}", "stop": f"{stop:.4f}", "R": f"{r:.4f}", "size": size})
                        return out


        # ---------- manage LONG ----------
        if s["pos"] == +1:
            entry = float(s["entry"]); stop = float(s["stop"]); r = float(s["r"])

            # Unified HA(5m) exit precedence (only if enabled)
            if self._maybe_exit_ha_5m(symbol, s, last, ts, close):
                return out

            # Management helpers (run every bar while in position)
            self._maybe_activate_be(symbol, s, last["timestamp"])
            self._apply_ratchet(symbol, s, float(last["close"]), last["timestamp"])
            if self._maybe_exit_ha(symbol, s, last):
                return

            # 1) hard stop
            if low <= stop:
                be_exit = s.get("be_active", False) and abs(stop - entry) <= max(1e-6, 1e-6 * entry)
                self._flat(symbol, reason="stop_hit", ts=ts)
                out.append({"action":"SELL", "symbol":symbol, "qty":"ALL", "type":"MKT",
                            "ts": ts, "reason":"stop_hit", "stop": stop})
                self._dbg(symbol, last, tag="EXIT_STOP", extra={"stop": f"{stop:.4f}", "low": f"{low:.4f}"})
                if be_exit and self.cfg.allow_reentry_after_be:
                    self.state[symbol]["allow_reentry_long"] = True
                return out

            # 2) trail stop by ATR
            new_stop = self._trail_stop(close, atrv, stop, direction=-1)
            if new_stop < stop:
                s["stop"] = self._set_stop("SHORT", symbol, ts, close, new_stop, s["entry"], stop, reason="trail_atr")

            # 3) partials  (LONG; cumulative-of-original)
            filled = int(s.get("took_partial", 0))
            moved  = (high - entry) / entry if entry else 0.0
            levels = list(self.cfg.partial_levels) if self.cfg.partial_levels else []

            # exit levels only if we still have size and price has moved enough
            while filled < len(levels) and s["size"] > 0:
                lvl = levels[filled]
                if moved < float(lvl["move_pct"]):
                    break

                # cumulative-of-original math
                orig = int(s.get("orig_size", s["size"]))
                exited_so_far = orig - s["size"]
                cum_frac = sum(float(levels[i]["exit_fraction"]) for i in range(filled + 1))
                target_exited = int(round(orig * cum_frac))
                qty = target_exited - exited_so_far


                # clamp & guard
                qty = max(0, min(qty, s["size"]))
                if qty <= 0:
                    break

                # --- DEBUG: cumulative math (pre-sell) ---
                if self.debug_partials:
                    self._log(
                        f"[PARTIAL DEBUG] lvl={filled+1} mv={float(lvl['move_pct']):.4f} "
                        f"orig={orig} sold_so_far={exited_so_far} desired_cum={target_exited} to_sell={qty}"
                    )

                s["size"] -= qty
                out.append({
                    "action": "SELL", "symbol": symbol, "qty": qty, "type": "MKT",
                    "ts": ts, "reason": f"partial_{filled+1}_at_{lvl['move_pct']*100:.2f}%"
                })
                self._dbg(symbol, last, tag="PARTIAL",
                          extra={"level": filled+1, "exit_frac": lvl["exit_fraction"], "qty": qty, "rem": s["size"]})

                filled += 1
                if self.cfg.partial_one_per_bar:
                    break

            s["took_partial"] = filled

            # if fully flat after partials, stop managing this leg
            if s["size"] == 0:
                self._flat(symbol, reason="all_partials_exit", ts=ts)
                return out

            # 4) breakeven activation after N partials
            if s["size"] > 0 and filled >= int(self.cfg.be_after_partials):
                proposed = max(s["stop"], entry)
                s["stop"]  = self._set_stop("SHORT", symbol, ts, close, proposed, entry, s["stop"], reason="be_after_partials")
                s["be_active"] = True

            # 5) bias exit (cross down or vwap loss + macd flip)
            bearish_cross = (p_ema9 >= p_ema20) and (ema9 < ema20)
            macd_flip     = macd_now < sig_now
            vwap_lost     = (not above_vwap) if self.cfg.vwap_filter else False
            if bearish_cross or (vwap_lost and macd_flip):
                self._flat(symbol, reason="bias_exit", ts=ts)
                out.append({"action":"SELL", "symbol":symbol, "qty":"ALL", "type":"MKT",
                            "ts": ts, "reason":"bias_exit"})
                self._dbg(symbol, last, tag="EXIT_BIAS")
                return out


        # ---------- manage SHORT ----------
        if s["pos"] == -1 and self.cfg.allow_shorts:
            entry = float(s["entry"]); stop = float(s["stop"]); r = float(s["r"])

            # Unified HA(5m) exit precedence (only if enabled)
            if self._maybe_exit_ha_5m(symbol, s, last, ts, close):
                return out
    def _maybe_exit_ha_5m(self, symbol, s, last, ts, close):
        """
        Unified HA(5m) exit for both LONG and SHORT. Always logs [SIG] EXIT_HA_5M.
        Returns True if an exit was triggered.
        """
        if not self.cfg_d.get("exits", {}).get("enable_ha_5m", True):
            return False
        ha_done = self.ha5m.update(ts, float(last["open"]), float(last["high"]), float(last["low"]), float(last["close"]))
        if not (ha_done and s.get("size", 0) > 0):
            return False
        ha_up = (ha_done["dir"] == "up")
        # LONG: exit if not up; SHORT: exit if up
        if (s.get("pos") == +1 and not ha_up) or (s.get("pos") == -1 and ha_up):
            self._log(f"[SIG] EXIT_HA_5M | sym={symbol} | ts={_fmt_ts(ha_done['ts'])} | close={close:.4f} | fill={close:.4f}")
            self._flat(symbol, reason="exit_ha_5m", ts=ha_done["ts"])
            return True
        return False

        return out or None

    # --- helpers --------------------------------------------------------------

    def _initial_stop(self, entry: float, atrv: float, *, direction: int) -> float:
        if self.cfg.stop_mode == "fixed":
            pct = float(self.cfg.fixed_sl_pct)
            if direction > 0:   # long
                return entry * (1.0 - pct)
            else:               # short
                return entry * (1.0 + pct)
        # ATR
        k = float(self.cfg.atr_mult)
        if direction > 0:
            return entry - k * atrv
        else:
            return entry + k * atrv

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
        import os, json

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

        # Dump payload—DO NOT scrub PnL fields
        payload = {
            "date": date,
            "symbols": syms,
            "risk_sizing": getattr(getattr(self, "config", None), "risk_sizing", None),
            "atr_mult": getattr(getattr(self, "config", None), "atr_mult", None),
            "log": list(getattr(self, "_log_lines", []) or []),
            "results": parsed,
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

        # Strict parity: emit fill=stop for EXIT_STOP, fill=minute close for PARTIAL/other EXIT_*
        if extra is None:
            extra = {}

        if tag == "PARTIAL":
            # parity: partials use the minute close as the fill
            extra.setdefault("fill", f"{float(last['close']):.4f}")

        elif tag == "EXIT_STOP":
            # parity with the old engine across both test days:
            stop_px  = float(extra.get("stop", last["close"]))
            close_px = float(last["close"])
            extra.setdefault("fill", f"{min(stop_px, close_px):.4f}")

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
