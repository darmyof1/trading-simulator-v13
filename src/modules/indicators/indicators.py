# src/modules/indicators/indicators.py
# pure functions: macd, atr, vwap, heikin_ashi, SMA/EMA bundles, etc.
from __future__ import annotations
import pandas as pd
import numpy as np

# --- SMA / EMA ----------------------------------------------------------------

def sma(s: pd.Series, window: int) -> pd.Series:
    """
    Simple Moving Average with full-window requirement (NaN until window bars exist).
    """
    return s.rolling(window=window, min_periods=window).mean()

def ema(s: pd.Series, span: int) -> pd.Series:
    """
    Exponential Moving Average (α = 2/(span+1)), seeded with min_periods=span.
    Matches common charting behavior when you preload >= span bars.
    """
    return s.ewm(span=span, adjust=False, min_periods=span).mean()

def ma_bundle(
    df: pd.DataFrame,
    price_col: str = "close",
    vol_col: str = "volume",
    ema_periods: tuple[int, ...] = (9, 20, 50, 200),
    sma_periods: tuple[int, ...] = (9, 20, 50, 200),
    include_volume: bool = True,
) -> pd.DataFrame:
    """
    Returns a DataFrame with EMA/SMA columns (and optional volume EMA/SMA).
    Columns added: ema_<n>, sma_<n>, [vol_ema_<n>, vol_sma_<n>].
    """
    out = pd.DataFrame(index=df.index)
    price = df[price_col].astype(float)

    # price MAs
    for n in ema_periods:
        out[f"ema_{n}"] = ema(price, n)
    for n in sma_periods:
        out[f"sma_{n}"] = sma(price, n)

    # volume MAs (optional)
    if include_volume and vol_col in df.columns:
        vol = df[vol_col].astype(float)
        for n in ema_periods:
            out[f"vol_ema_{n}"] = vol.ewm(span=n, adjust=False, min_periods=n).mean()
        for n in sma_periods:
            out[f"vol_sma_{n}"] = vol.rolling(window=n, min_periods=n).mean()

    return out

# --- MACD ---------------------------------------------------------------------

def macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
    macd_line = ema(close, fast) - ema(close, slow)
    signal_line = ema(macd_line, signal)
    hist = macd_line - signal_line
    return pd.DataFrame({"macd": macd_line, "macd_signal": signal_line, "macd_hist": hist})

# --- ATR (Wilder) -------------------------------------------------------------

def atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low),
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)
    # Wilder smoothing: first value is simple mean, then recursive
    return tr.ewm(alpha=1/period, adjust=False, min_periods=period).mean()

# --- Heikin Ashi --------------------------------------------------------------

def heikin_ashi(df: pd.DataFrame) -> pd.DataFrame:
    """
    Expects columns: open, high, low, close. Returns HA columns alongside.
    """
    ha = pd.DataFrame(index=df.index)
    ha["ha_close"] = (df["open"] + df["high"] + df["low"] + df["close"]) / 4.0
    ha["ha_open"] = (df["open"].shift(1) + df["close"].shift(1)) / 2.0
    ha.loc[ha["ha_open"].isna(), "ha_open"] = (df["open"] + df["close"]) / 2.0
    ha["ha_high"] = pd.concat([df["high"], ha["ha_open"], ha["ha_close"]], axis=1).max(axis=1)
    ha["ha_low"]  = pd.concat([df["low"],  ha["ha_open"], ha["ha_close"]], axis=1).min(axis=1)
    return ha

# --- VWAP (rolling cumulative) ------------------------------------------------

def vwap(df: pd.DataFrame, price_col: str = "close", vol_col: str = "volume") -> pd.Series:
    """
    Session VWAP if you reset cumulatives externally at 09:30 ET.
    For continuous VWAP, just run across the whole day.
    """
    typical = (df["high"] + df["low"] + df["close"]) / 3.0 if {"high","low","close"}.issubset(df.columns) else df[price_col]
    cum_pv = (typical * df[vol_col]).cumsum()
    cum_v  = (df[vol_col]).cumsum().replace(0, np.nan)
    return (cum_pv / cum_v).ffill()
