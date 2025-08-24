# src/modules/datafeeds/common.py
from __future__ import annotations
from enum import Enum
from typing import List, Optional
from pathlib import Path
import os

class IngressMode(str, Enum):
    DISK = "disk"     # write files only
    DIRECT = "direct" # call a callback only (no files)
    TEE = "tee"       # both: files + callback

def load_symbols(symbols: Optional[str], symbols_file: Optional[str]) -> List[str]:
    out: List[str] = []
    if symbols:
        out += [s.strip().upper() for s in symbols.split(",") if s.strip()]
    if symbols_file:
        p = Path(symbols_file)
        if not p.exists():
            raise FileNotFoundError(f"symbols file not found: {symbols_file}")
        text = p.read_text(encoding="utf-8")
        # allow comma- or newline-separated lists
        raw = [x.strip().upper() for chunk in text.splitlines() for x in chunk.split(",")]
        out += [s for s in raw if s]
    # de-dupe, keep order
    dedup: List[str] = []
    seen = set()
    for s in out:
        if s not in seen:
            dedup.append(s); seen.add(s)
    if not dedup:
        raise ValueError("No symbols provided. Use --symbols or --symbols-file.")
    return dedup

def default_out(provider: str) -> str:
    """
    Default to ./data/output/<provider>, or $DATA_OUT_ROOT/<provider> if the env is set.
    """
    root = os.getenv("DATA_OUT_ROOT", "./data/output")
    return str(Path(root) / provider)

def ensure_out_dir(path: str) -> str:
    Path(path).mkdir(parents=True, exist_ok=True)
    return path
