# dataclass + loader (yaml/json)
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
import json
import pathlib

try:
    import yaml  # optional
except Exception:
    yaml = None

@dataclass
class StrategyConfig:
    results_dump_path: Optional[str] = None
    results_run_label: Optional[str] = "risk"

    # --- sizing (new) ---
    use_risk_sizing: bool = False         # default OFF to preserve current behavior
    account_balance: float = 25000.0      # will be replaced by broker balance later
    risk_pct_of_balance: float = 0.005    # 0.5% risk per trade
    min_size: int = 1
    max_size: int = 10000
    lot_size: int = 1                     # round size down to this multiple
    # === existing fields ===
    atr_mult: float = 2.0
    partial_levels: List[Dict[str, Any]] = field(default_factory=list)
    partial_one_per_bar: bool = False
    be_after_partials: int = 0
    warmup_secs: int = 12_000
    debug_signals: bool = True
    base_size: int = 4  # use 4 to mirror the old 1,1,2 partial behavior
    ratchet_pct_short: float = 4.0
    ratchet_pct_long: float = 4.0

    # === NEW fields ===
    # Stops
    stop_mode: str = "atr"           # "atr" | "fixed"
    fixed_sl_pct: float = 0.010      # 1.0% fixed stop if stop_mode="fixed"

    # Entries / filters
    vwap_filter: bool = True         # require above VWAP for longs / below VWAP for shorts
    allow_shorts: bool = True        # enable symmetrical short logic

    # Trade frequency
    max_trades_per_dir: int = 1      # 1 = only one entry per direction per day
    allow_reentry_after_be: bool = True  # allow exactly one re-entry after a breakeven exit

    # --- results output ---
    # results_dump_path: Optional[str] = None  # already above

    # --- NEW generic fields for v13.py and future knobs ---
    entry_window: Dict[str, Any] = field(default_factory=dict)
    be: Dict[str, Any] = field(default_factory=dict)
    ratchet: Dict[str, Any] = field(default_factory=dict)
    ha_exit: Dict[str, Any] = field(default_factory=dict)

    # (optional) a parking lot for truly unknown keys
    extra: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.partial_levels:
            self.partial_levels = [
                {"move_pct": 0.005, "exit_fraction": 0.25},
                {"move_pct": 0.010, "exit_fraction": 0.25},
                {"move_pct": 0.020, "exit_fraction": 0.50},
            ]

def load_config(path: Optional[str]) -> StrategyConfig:
    if not path:
        return StrategyConfig()
    p = pathlib.Path(path)
    text = p.read_text()
    if p.suffix.lower() in {".yaml", ".yml"}:
        if not yaml:
            raise RuntimeError("pyyaml not installed; pip install pyyaml or use JSON")
        data = yaml.safe_load(text)
    else:
        data = json.loads(text)

    # allow unknown keys without crashing
    from typing import get_type_hints
    known = set(get_type_hints(StrategyConfig).keys())
    known_data = {k: v for k, v in data.items() if k in known}
    unknown    = {k: v for k, v in data.items() if k not in known}

    cfg = StrategyConfig(**known_data)
    if hasattr(cfg, "extra"):
        cfg.extra.update(unknown)
    return cfg
