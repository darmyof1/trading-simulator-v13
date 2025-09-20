# src/tests/test_v13_parity.py
import json, pathlib, subprocess, sys

def test_vsee_parity():
    exp = json.loads(pathlib.Path("docs/expected/StrategyV13_2025-07-14_VSEE.json").read_text())
    got = json.loads(pathlib.Path("logs/results/StrategyV13_2025-07-14_VSEE_risk.json").read_text())
    assert got["results"] == exp["results"]

def test_hubc_parity():
    exp = json.loads(pathlib.Path("docs/expected/StrategyV13_2025-07-14_HUBC.json").read_text())
    got = json.loads(pathlib.Path("logs/results/StrategyV13_2025-07-14_HUBC_risk.json").read_text())
    assert got["results"] == exp["results"]
