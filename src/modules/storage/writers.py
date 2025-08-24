# src/modules/storage/writers.py
# writers.py – Disk I/O layer, provider-agnostic:
#   - write_ohlcv_csv(df, csv_path)
#   - save_parquet_append(df, outdir, date_str, symbol, feed_label)
# singleton you can import and reuse

from __future__ import annotations
import os
import pandas as pd
from pathlib import Path
import pyarrow as pa, pyarrow.parquet as pq



# Parquet session manager for efficient Parquet appends
class ParquetSessionManager:
    """
    Keeps open ParquetWriter(s) keyed by (outdir, date, symbol, label).
    Appends row groups without re-reading the file.
    """
    def __init__(self):
        self._writers = {}  # key -> (writer, schema)

    def _key(self, outdir, date_str, symbol, label):
        return (Path(outdir).resolve(), date_str, symbol, label)

    def append(self, df, outdir, date_str, symbol, label):
        key = self._key(outdir, date_str, symbol, label)
        table = pa.Table.from_pandas(df, preserve_index=False)
        if key not in self._writers:
            path = Path(outdir)/f"{symbol}_{date_str}_{label}_ohlcv-1s.parquet"
            writer = pq.ParquetWriter(path, table.schema, compression="snappy")
            self._writers[key] = (writer, table.schema)
        writer, schema = self._writers[key]
        # (Optional) enforce schema compatibility:
        if table.schema != schema:
            table = table.cast(schema)
        writer.write_table(table)

    def close_all(self):
        for writer, _ in self._writers.values():
            try: writer.close()
            except: pass
        self._writers.clear()

parquet_sessions = ParquetSessionManager()


def write_ohlcv_csv(df: pd.DataFrame, csv_path: str) -> None:
    """
    Append OHLCV rows to a CSV (no header). Expected columns:
        timestamp, open, high, low, close, volume
    """
    first_write = not os.path.exists(csv_path)
    df.to_csv(csv_path, index=False, header=False, mode='w' if first_write else 'a')

def save_parquet_append(df: pd.DataFrame, outdir: str, date_str: str, symbol: str, feed_label: str) -> None:
    """
    Append to a daily parquet: {SYMBOL}_{DATE}_{LABEL}_ohlcv-1s.parquet using ParquetSessionManager.
    """
    parquet_sessions.append(df, outdir, date_str, symbol, feed_label)
def close_parquet_sessions():
    parquet_sessions.close_all()