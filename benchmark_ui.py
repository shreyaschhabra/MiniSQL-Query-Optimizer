"""
benchmark_ui.py
---------------
Task 3: Measure the latency of generating the full visual tree output
(ASCII annotated query tree via PlanVisualizer) for the complex multi-join query.

Runs 100 iterations and reports average + percentiles.
"""

import sys
import time

sys.path.insert(0, "/Users/shreyaschhabra/Desktop/DBMS-Project-test")

from engine.catalog    import Catalog
from engine.parser     import QueryParser
from engine.rbo        import RuleBasedOptimizer
from engine.cbo        import CostBasedOptimizer
from engine.visualizer import PlanVisualizer

# ── Same complex 5-way JOIN query used in Task 1 ─────────────────────────────
COMPLEX_SQL = """
WITH high_value_orders AS (
    SELECT
        olist_order_payments_dataset.order_id,
        olist_order_payments_dataset.payment_value,
        olist_order_payments_dataset.payment_type
    FROM olist_order_payments_dataset
    WHERE olist_order_payments_dataset.payment_value > 200
      AND olist_order_payments_dataset.payment_installments >= 3
)
SELECT
    olist_customers_dataset.customer_state,
    olist_sellers_dataset.seller_state,
    olist_products_dataset.product_category_name,
    COUNT(olist_orders_dataset.order_id)   AS total_orders,
    SUM(high_value_orders.payment_value)   AS total_revenue,
    AVG(olist_order_items_dataset.price)   AS avg_item_price
FROM olist_orders_dataset
INNER JOIN olist_customers_dataset
    ON olist_orders_dataset.customer_id = olist_customers_dataset.customer_id
INNER JOIN olist_order_items_dataset
    ON olist_orders_dataset.order_id = olist_order_items_dataset.order_id
INNER JOIN olist_products_dataset
    ON olist_order_items_dataset.product_id = olist_products_dataset.product_id
INNER JOIN olist_sellers_dataset
    ON olist_order_items_dataset.seller_id = olist_sellers_dataset.seller_id
INNER JOIN high_value_orders
    ON olist_orders_dataset.order_id = high_value_orders.order_id
WHERE olist_orders_dataset.order_status = 'delivered'
  AND (olist_customers_dataset.customer_state = 'SP'
       OR olist_customers_dataset.customer_state = 'RJ')
  AND olist_products_dataset.product_category_name IS NOT NULL
GROUP BY
    olist_customers_dataset.customer_state,
    olist_sellers_dataset.seller_state,
    olist_products_dataset.product_category_name
HAVING COUNT(olist_orders_dataset.order_id) > 10
"""

RUNS = 100

def main():
    catalog    = Catalog()
    parser     = QueryParser()
    rbo        = RuleBasedOptimizer(catalog=catalog)
    cbo        = CostBasedOptimizer(catalog=catalog)
    visualizer = PlanVisualizer()

    # ── Build the final optimized plan once (outside the timer) ──────────────
    raw_tree = parser.parse(COMPLEX_SQL)
    rbo_tree = rbo.optimize(raw_tree)
    cbo_result = cbo.optimize(rbo_tree)
    optimized_plan = cbo_result.plan

    # ── Warm-up ───────────────────────────────────────────────────────────────
    visualizer.render(optimized_plan)

    # ── Benchmark the visualizer render step only ─────────────────────────────
    latencies = []
    for _ in range(RUNS):
        t0 = time.perf_counter()
        tree_str = visualizer.render(optimized_plan)
        t1 = time.perf_counter()
        latencies.append((t1 - t0) * 1_000)   # → ms

    avg_ms = sum(latencies) / len(latencies)
    min_ms = min(latencies)
    max_ms = max(latencies)
    p95_ms = sorted(latencies)[int(0.95 * RUNS) - 1]

    # Count AST nodes (tree lines) for resume metrics
    node_count = len([l for l in tree_str.split("\n") if l.strip()])

    print(f"\n[Task 3] Plan Visualization / UI Render Latency  ({RUNS} runs)")
    print(f"  Rendered nodes : {node_count} AST nodes in output tree")
    print(f"  Average        : {avg_ms:.4f} ms")
    print(f"  Min            : {min_ms:.4f} ms")
    print(f"  Max            : {max_ms:.4f} ms")
    print(f"  P95            : {p95_ms:.4f} ms")

    return avg_ms, node_count

if __name__ == "__main__":
    main()
