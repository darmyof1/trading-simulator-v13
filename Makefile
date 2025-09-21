run-vsee:
\tpython3 -m src.modules.backtest.runner --symbols VSEE --dates 2025-07-14 \
\t  --strategy src.modules.strategy.v13:StrategyV13 --config configs/strategy_v13.json \
\t  --canonical-root ./data/output --timeframe 1s --warmup-mins 200 --sessions extended

run-hubc:
\tpython3 -m src.modules.backtest.runner --symbols HUBC --dates 2025-07-14 \
\t  --strategy src.modules.strategy.v13:StrategyV13 --config configs/strategy_v13.json \
\t  --canonical-root ./data/output --timeframe 1s --warmup-mins 200 --sessions extended

parity-vsee:
\tpython3 scripts/compare_v13_dumps.py docs/expected/StrategyV13_2025-07-14_VSEE.json logs/results/StrategyV13_2025-07-14_VSEE_risk.json

parity-hubc:
\tpython3 scripts/compare_v13_dumps.py docs/expected/StrategyV13_2025-07-14_HUBC.json logs/results/StrategyV13_2025-07-14_HUBC_risk.json
