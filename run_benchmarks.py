"""
run_benchmarks.py
-----------------
Master benchmark runner.
Executes Tasks 1, 2, and 3 in sequence and prints a clean summary table.
"""

import sys
sys.path.insert(0, "/Users/shreyaschhabra/Desktop/DBMS-Project-test")

# ── Task 1 ────────────────────────────────────────────────────────────────────
print("=" * 60)
print("  Running Task 1: AST Parsing Latency ...")
print("=" * 60)
import benchmark_parser
parse_avg_ms = benchmark_parser.main()

# ── Task 2 ────────────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("  Running Task 2: CBO vs RBO Cost Reduction ...")
print("=" * 60)
import benchmark_cbo
pct_reduction, rbo_cost, cbo_cost, best_order = benchmark_cbo.main()

# ── Task 3 ────────────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("  Running Task 3: Plan Visualization / UI Latency ...")
print("=" * 60)
import benchmark_ui
ui_avg_ms, node_count = benchmark_ui.main()

# ── FINAL SUMMARY TABLE ───────────────────────────────────────────────────────
W = 62
print("\n")
print("█" * W)
print(f"{'  BENCHMARK RESULTS — SQL QUERY OPTIMIZER':^{W}}")
print("█" * W)
print(f"  {'Metric':<42} {'Result':>14}")
print("─" * W)
print(f"  {'AST Parse Time  (complex 5-way JOIN + CTE)':<42} {parse_avg_ms:>12.4f} ms")
print("─" * W)
print(f"  {'CBO Cost Reduction vs RBO  (1M–5M row sim.)':<42} {pct_reduction:>11.2f} %")
print(f"  {'  RBO naive plan cost':<42} {rbo_cost:>14,}")
print(f"  {'  CBO optimal plan cost':<42} {cbo_cost:>14,}")
print(f"  {'  Optimal join order':<42} {' ⋈ '.join(best_order)[:14]:>14}")
print("─" * W)
print(f"  {'Plan Generation / UI Render Latency':<42} {ui_avg_ms:>12.4f} ms")
print(f"  {'  AST nodes rendered in output tree':<42} {node_count:>14}")
print("█" * W)
print()
print("  Resume-ready numbers:")
print(f"    • AST parse time          : {parse_avg_ms:.2f} ms  (avg over 100 runs)")
print(f"    • CBO cost reduction      : {pct_reduction:.1f}%  vs RBO (1M–5M row tables)")
print(f"    • UI / plan render time   : {ui_avg_ms:.2f} ms  ({node_count} nodes rendered)")
print()
