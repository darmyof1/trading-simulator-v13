# src/modules/utils/time.py
import pandas as pd

def to_utc(ts):
    t = pd.Timestamp(ts)
    return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")


def to_utc_index(df: pd.DataFrame) -> pd.DataFrame:
    idx = df.index
    if idx.tz is None:
        df = df.tz_localize('UTC')
    else:
        df = df.tz_convert('UTC')
    return df.sort_index()

def ensure_monotonic(df: pd.DataFrame) -> pd.DataFrame:
    return df[~df.index.duplicated(keep='first')].sort_index()
