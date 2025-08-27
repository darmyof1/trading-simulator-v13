# StrategyBase: on_start, on_bar, on_end
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any, Iterable, Protocol, Optional
import pandas as pd

# Canonical bar shape used everywhere (UTC timestamps, 6 columns)
Bar = Dict[str, Any]  # keys: timestamp (pd.Timestamp tz-aware UTC), open, high, low, close, volume, optional 'source'

@dataclass
class StrategyContext:
    date: str                     # "YYYY-MM-DD"
    symbols: list[str]
    config: Any                   # a StrategyConfig (defined in config.py)
    extras: Dict[str, Any] = None # optional (paths, handles, etc.)

class StrategyBase(Protocol):
    """Every strategy must implement these three hooks."""
    def on_start(self, ctx: StrategyContext) -> None: ...
    def on_bar(self, symbol: str, bar: Bar) -> Optional[Iterable[Dict[str, Any]]]: ...
    def on_end(self) -> None: ...
    def on_quote(self, symbol: str, quote: dict) -> None:
        """Optional: handle live quote events (default: no-op)."""
        pass
