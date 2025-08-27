from __future__ import annotations
from typing import Dict, Any, Iterable, Optional, List
import pandas as pd
import numpy as np

from modules.strategy.base import StrategyBase, StrategyContext
from modules.strategy.config import StrategyConfig
from modules.indicators.indicators import ma_bundle, macd, atr, vwap

Bar = Dict[str, Any]

class StrategyV13(StrategyBase):

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

    def __init__(self, config: StrategyConfig):
        self.cfg = config
        self.ctx: Optional[StrategyContext] = None
        self.buffers: Dict[str, pd.DataFrame] = {}
        # per-symbol position & accounting
        self.state: Dict[str, Dict[str, Any]] = {}

    def on_start(self, ctx: StrategyContext) -> None:
        self.ctx = ctx
        for sym in ctx.symbols:
            self.buffers[sym] = pd.DataFrame(columns=["timestamp","open","high","low","close","volume"])
            self.state[sym] = {
                "pos": 0,                     # -1 short, 0 flat, +1 long
                "entry": None,
                "stop": None,
                "r": None,
                "size": 0,
                "orig_size": 0,               # track original size on entry
                "took_partial": 0,
                "be_active": False,           # whether BE is set (stop==entry)
                "allow_reentry_long": False,  # set true after a BE exit to permit one more long
                "allow_reentry_short": False, # set true after a BE exit to permit one more short
                "entries_long": 0,            # entries taken today in long dir
                "entries_short": 0,           # entries taken today in short dir
            }
        print(f"[StrategyV13] start date={ctx.date} symbols={ctx.symbols}")

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
            new_stop = self._trail_stop(close, atrv, stop, direction=+1)
            if new_stop > stop:
                s["stop"] = new_stop

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
                s["stop"] = max(s["stop"], entry)
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
            # 1) hard stop (for short, stop is above—hit if high >= stop)
            if high >= stop:
                be_exit = s.get("be_active", False) and abs(stop - entry) <= max(1e-6, 1e-6 * entry)
                self._flat(symbol, reason="stop_hit", ts=ts)
                out.append({"action":"COVER", "symbol":symbol, "qty":"ALL", "type":"MKT",
                            "ts": ts, "reason":"stop_hit", "stop": stop})
                self._dbg(symbol, last, tag="EXIT_STOP", extra={"stop": f"{stop:.4f}", "high": f"{high:.4f}"})
                if be_exit and self.cfg.allow_reentry_after_be:
                    self.state[symbol]["allow_reentry_short"] = True
                return out

            # 2) trail stop by ATR (for short, trail above price, only downwards)
            new_stop = self._trail_stop(close, atrv, stop, direction=-1)
            if new_stop < stop:
                s["stop"] = new_stop

            # 3) partials  (SHORT; cumulative-of-original)
            filled = int(s.get("took_partial", 0))
            moved  = (entry - low) / entry if entry else 0.0
            levels = list(self.cfg.partial_levels) if self.cfg.partial_levels else []

            while filled < len(levels) and s["size"] > 0:
                lvl = levels[filled]
                if moved < float(lvl["move_pct"]):
                    break

                orig = int(s.get("orig_size", s["size"]))
                exited_so_far = orig - s["size"]
                cum_frac = sum(float(levels[i]["exit_fraction"]) for i in range(filled + 1))
                target_exited = int(round(orig * cum_frac))
                qty = target_exited - exited_so_far

                qty = max(0, min(qty, s["size"]))
                if qty <= 0:
                    break

                s["size"] -= qty
                out.append({
                    "action": "COVER", "symbol": symbol, "qty": qty, "type": "MKT",
                    "ts": ts, "reason": f"partial_{filled+1}_at_{lvl['move_pct']*100:.2f}%"
                })
                self._dbg(symbol, last, tag="PARTIAL",
                          extra={"level": filled+1, "exit_frac": lvl["exit_fraction"], "qty": qty, "rem": s["size"]})

                filled += 1
                if self.cfg.partial_one_per_bar:
                    break

            s["took_partial"] = filled

            if s["size"] == 0:
                self._flat(symbol, reason="all_partials_exit", ts=ts)
                return out

            # 4) BE activation after N partials
            if s["size"] > 0 and filled >= int(self.cfg.be_after_partials):
                s["stop"] = min(s["stop"], entry)
                s["be_active"] = True

            # 5) bias exit for short (cross up or vwap regain + macd flip up)
            bullish_cross = (p_ema9 <= p_ema20) and (ema9 > ema20)
            macd_flip_up  = macd_now > sig_now
            vwap_regain   = above_vwap if self.cfg.vwap_filter else False
            if bullish_cross or (vwap_regain and macd_flip_up):
                self._flat(symbol, reason="bias_exit", ts=ts)
                out.append({"action":"COVER", "symbol":symbol, "qty":"ALL", "type":"MKT",
                            "ts": ts, "reason":"bias_exit"})
                self._dbg(symbol, last, tag="EXIT_BIAS")
                return out

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

    def on_end(self) -> None:
        print("[StrategyV13] end")

    def on_quote(self, symbol: str, quote: dict) -> None:
        pass

    def _dbg(self, symbol: str, last: pd.Series, *, tag: str, extra: dict | None = None) -> None:
        if not getattr(self.cfg, "debug_signals", True):
            return
        def f(x):
            try: return f"{float(x):.4f}"
            except Exception: return str(x)
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
        print(" | ".join(parts), flush=True)
