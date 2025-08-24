# src/modules/datafeeds/databento_data.py
from __future__ import annotations
import argparse
from modules.datafeeds.common import load_symbols, ensure_out_dir, IngressMode, default_out

def premarket(symbols, date_str, outdir, label):
    print(f"[DATABENTO] (stub) premarket {date_str} {symbols} → {outdir}/{label}_*")

def stream(symbols, date_str, outdir, label, ingress: IngressMode):
    print(f"[DATABENTO] (stub) stream {symbols} date={date_str} ingress={ingress} → {outdir}/{label}_*")

def _add_common(ap: argparse.ArgumentParser) -> argparse.ArgumentParser:
    ap.add_argument("--symbols")
    ap.add_argument("--symbols-file")
    ap.add_argument("--date", required=True)
    ap.add_argument("--out", default=default_out("databento"))
    ap.add_argument("--label", default="DB")
    ap.add_argument("--ingress", choices=[m.value for m in IngressMode], default=IngressMode.DISK.value)
    return ap

def main():
    top = argparse.ArgumentParser("DataBento Data CLI")
    sub = top.add_subparsers(dest="cmd", required=True)
    _add_common(sub.add_parser("premarket"))
    _add_common(sub.add_parser("stream"))
    args = top.parse_args()

    symbols = load_symbols(args.symbols, args.symbols_file)
    outdir = ensure_out_dir(args.out)
    ingress = IngressMode(args.ingress)

    if args.cmd == "premarket":
        premarket(symbols, args.date, outdir, args.label)
    else:
        stream(symbols, args.date, outdir, args.label, ingress)

if __name__ == "__main__":
    main()
