from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, time, timezone
from typing import Any, Dict, List, Mapping, Optional

try:
    from zoneinfo import ZoneInfo
    NY = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover
    NY = None  # type: ignore

__all__ = ["StrategyContext", "StrategyBase", "Bar", "get_logger"]

Bar = Mapping[str, Any]


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(h)
    logger.propagate = False
    return logger


@dataclass(slots=True)
class StrategyContext:
    date: str | datetime
    symbols: List[str]
    timeframe: str = "1m"
    session: str = "regular"      # "regular" | "extended"
    config: Any = None
    tz: Optional[Any] = NY
    extras: Dict[str, Any] = field(default_factory=dict)


class StrategyBase:
    ctx: Optional[StrategyContext]
    cfg: Any                    # StrategyConfig or dict-like
    cfg_d: Dict[str, Any]       # plain dict mirror for legacy code
    _log: logging.Logger
    _now_override: Optional[datetime]
    _tz: Optional[Any]

    def __init__(self, ctx_or_cfg: Any):
        """
        Accepts StrategyContext, StrategyConfig, or a dict.
        - If StrategyContext: keep it and take .config as raw config
        - If StrategyConfig: use as-is
        - If dict: wrap into StrategyConfig but also keep a dict mirror
        """
        # Normalize "raw_cfg"
        if hasattr(ctx_or_cfg, "symbols") and hasattr(ctx_or_cfg, "config"):
            # StrategyContext
            self.ctx = ctx_or_cfg
            raw_cfg = ctx_or_cfg.config
        else:
            self.ctx = None
            raw_cfg = ctx_or_cfg

        # Construct self.cfg and self.cfg_d
        try:
            from .config import StrategyConfig  # local import to avoid cycles
        except Exception:
            StrategyConfig = None  # type: ignore

        if StrategyConfig is not None and isinstance(raw_cfg, StrategyConfig):  # type: ignore[arg-type]
            self.cfg = raw_cfg
        elif isinstance(raw_cfg, dict):
            # wrap dict into StrategyConfig if available; else keep dict
            self.cfg = StrategyConfig(raw_cfg) if StrategyConfig is not None else raw_cfg  # type: ignore[call-arg]
        else:
            # unknown object (already a StrategyConfig-like), keep as-is
            self.cfg = raw_cfg

        self.cfg_d = self._to_plain_dict(self.cfg)

        self._tz = getattr(self.ctx, "tz", NY) if self.ctx is not None else NY
        self._now_override = None
        self._log = get_logger(self.__class__.__name__)

    # ---------------- time helpers ----------------
    def set_now(self, dt: datetime | None) -> None:
        self._now_override = dt

    def _now_utc(self) -> datetime:
        return (self._now_override or datetime.now(timezone.utc)).astimezone(timezone.utc)

    def _now_iso(self) -> str:
        return self._now_utc().isoformat()

    # --------------- session helper ---------------
    @staticmethod
    def _parse_tod(v: str) -> time:
        hh, mm = v.split(":")
        return time(int(hh), int(mm))

    def is_within_entry_window(self, ts: datetime, window: str = "09:30-16:00") -> bool:
        start_s, end_s = window.split("-")
        start_t = self._parse_tod(start_s)
        end_t = self._parse_tod(end_s)

        if ts.tzinfo is None:
            ts_utc = ts.replace(tzinfo=timezone.utc)
        else:
            ts_utc = ts.astimezone(timezone.utc)

        tz = self._tz or timezone.utc
        ts_local = ts_utc.astimezone(tz)
        tod = ts_local.timetz().replace(tzinfo=None)
        return start_t <= tod <= end_t

    # -------------- misc small helper -------------
    @staticmethod
    def simulate_stop_fill(side: str, stop: float, low: float, high: float) -> bool:
        if side == "long":
            return low <= stop
        if side == "short":
            return high >= stop
        raise ValueError(f"unknown side={side!r}")

    # -------------- private utils -----------------
    @staticmethod
    def _to_plain_dict(cfg_obj: Any) -> Dict[str, Any]:
        """Best-effort conversion of config object to a plain dict for legacy access."""
        if isinstance(cfg_obj, dict):
            return dict(cfg_obj)

        # Try common adapters
        for name in ("data", "as_dict", "to_dict", "dict"):
            if hasattr(cfg_obj, name):
                try:
                    v = getattr(cfg_obj, name)
                    v = v() if callable(v) else v
                    if isinstance(v, dict):
                        return dict(v)
                except Exception:
                    pass

        # Last resort: collect public non-callable attributes
        try:
            out = {}
            for k in dir(cfg_obj):
                if k.startswith("_"):
                    continue
                try:
                    val = getattr(cfg_obj, k)
                except Exception:
                    continue
                if not callable(val):
                    out[k] = val
            return out
        except Exception:
            return {}
