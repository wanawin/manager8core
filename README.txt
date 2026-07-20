NS CORE SET LAB v51.29

RUN
1. Extract the ZIP to a short Windows path.
2. Double-click RUN_APP.bat.
3. Upload history.
4. Open the walk-forward backtest.
5. First run exactly 2 dates with Compare winner across v51.26 charts enabled.
6. Leave Track member accuracy OFF for the speed test.
7. Final Boxed may remain ON; runtime_by_date.csv will show its separate cost.
8. Download all Backtest outputs.

TEST SEQUENCE
- 2 dates: correctness and runtime acceptance.
- 10 dates: early pattern and scaling check.
- 20 dates: stronger pattern check.
- Expand further only after runtime and signal justify it.

OUTPUTS
- walkforward_rows.csv / walkforward_winner_rows.csv
- by_core.csv
- by_stream.csv
- runtime_by_date.csv
- optional member strategy files when member tracking is enabled

IMPORTANT
The supplied NORTHERN_STAR_ALL_SELECTED_2026-06-21_v51.28a.zip is a normal daily-output ZIP. It contains per-core daily charts but no walk-forward backtest ledger, so it cannot answer the multi-date winner-location question by itself.


V51.32 WALK-FORWARD GATE OPTIONS
- Current hard gate: Base/Due only
- Open gate: all rows
- Flipped gate: NOT Base/Due only
The flipped option is audit-only and does not alter the normal daily production run.
