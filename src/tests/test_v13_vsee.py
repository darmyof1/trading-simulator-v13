import json, pathlib, os, pytest

# Skip on CI or if the data file isn't present locally
if os.getenv("SKIP_SLOW_PARITY", "0") == "1":
    pytest.skip("Skipping parity test on CI", allow_module_level=True)

DATA_FILE = pathlib.Path("data/output/canonical/VSEE/VSEE_2025-07-14_canonical_ohlcv-1s.csv")
if not DATA_FILE.exists():
    pytest.skip(f"Missing local data file: {DATA_FILE}", allow_module_level=True)

def test_v13_vsee_2025_07_14():
    p = pathlib.Path("logs/results/StrategyV13_2025-07-14_VSEE_risk.json")
    assert p.exists(), "Run the backtest first."
    data = json.loads(p.read_text())
    # Example expectations from your last run — tweak if your config changes sizing/triggers:
    assert data["symbol"] == "VSEE"
    assert data["date"] == "2025-07-14"
    assert data["parsed_trades"] == 1
    t = data["trades"][0]
    assert t["side"] == "SHORT"
    assert t["entries"] == 1
    assert t["partials"] >= 1
    assert t["exit_reason"] in ("STOP", "HA_STOP", "EXIT_HA_STOP")
