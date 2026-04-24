"""
benchmark_cbo.py
----------------
Task 2: CBO vs RBO cost reduction on a massive 5-table, 1M+ row simulated catalog.

Methodology:
  - Inject 5 giant tables into the Catalog (each 1M–5M rows).
  - Run both the RBO pipeline and the CBO pipeline on the same multi-join query.
  - RBO cost  = cost computed for the *original* left-to-right join order.
  - CBO cost  = cost computed for the *best* permutation chosen by CBO.
  - Reduction = (rbo_cost - cbo_cost) / rbo_cost * 100
"""

import sys

sys.path.insert(0, "/Users/shreyaschhabra/Desktop/DBMS-Project-test")

from engine.catalog  import Catalog
from engine.parser   import QueryParser
from engine.rbo      import RuleBasedOptimizer
from engine.cbo      import CostBasedOptimizer
import itertools, re


# ── 1.  Build a massively-sized simulated catalog ────────────────────────────
def build_large_catalog() -> Catalog:
    cat = Catalog()

    # Wipe defaults and inject 5 big tables
    for tbl in list(cat._tables.keys()):
        cat.remove_table(tbl)

    cat.add_table("orders",    row_count=5_000_000,
                  columns=["order_id", "customer_id", "product_id", "status", "total_amount"])
    cat.add_table("customers", row_count=2_500_000,
                  columns=["customer_id", "name", "region_id", "segment"])
    cat.add_table("products",  row_count=1_200_000,
                  columns=["product_id", "category_id", "price", "stock_qty"])
    cat.add_table("regions",   row_count=1_000,
                  columns=["region_id", "region_name", "country"])
    cat.add_table("categories",row_count=500,
                  columns=["category_id", "category_name", "department"])
    return cat


# ── 2. Complex 5-table JOIN query (all INNER — CBO can freely reorder) ───────
MULTI_JOIN_SQL = """
SELECT
    customers.region_id,
    categories.category_name,
    COUNT(orders.order_id)    AS total_orders,
    SUM(orders.total_amount)  AS revenue,
    AVG(products.price)       AS avg_price
FROM orders
INNER JOIN customers   ON orders.customer_id  = customers.customer_id
INNER JOIN products    ON orders.product_id   = products.product_id
INNER JOIN regions     ON customers.region_id = regions.region_id
INNER JOIN categories  ON products.category_id = categories.category_id
WHERE orders.status = 'completed'
  AND customers.segment = 'premium'
  AND products.stock_qty > 0
GROUP BY customers.region_id, categories.category_name
HAVING COUNT(orders.order_id) > 50
"""


# ── 3.  Helpers to compute cost for an explicit table ordering ───────────────
def _tables_in_expr(expr: str):
    matches = re.findall(r"([A-Za-z_]\w*)\.(?:[A-Za-z_]\w*)", expr)
    return {t.lower() for t in matches}

def compute_original_cost(table_order, join_conditions, catalog: Catalog) -> int:
    """Replicate CBO's _compute_order_cost() for the original query order."""
    cardinalities = {}
    for tbl in table_order:
        try:
            cardinalities[tbl] = catalog.get_cardinality(tbl)
        except KeyError:
            cardinalities[tbl] = 1

    intermediate = cardinalities[table_order[0]]
    total        = 0
    introduced   = [table_order[0]]
    used_conds   = set()

    for tbl in table_order[1:]:
        introduced.append(tbl)
        matched = None
        for cond in join_conditions:
            if cond in used_conds:
                continue
            mentioned = _tables_in_expr(cond)
            if mentioned and mentioned.issubset(set(introduced)):
                matched = cond
                break

        if matched:
            used_conds.add(matched)
            step         = int(intermediate * cardinalities[tbl] * 0.1)
            total       += step
            intermediate = max(intermediate, cardinalities[tbl])
        else:
            step         = intermediate * cardinalities[tbl]
            total       += step
            intermediate = step

    return total


def main():
    catalog = build_large_catalog()
    parser  = QueryParser()
    rbo     = RuleBasedOptimizer(catalog=catalog)
    cbo     = CostBasedOptimizer(catalog=catalog)

    # ── Parse a FRESH (unoptimized) tree to capture the original writer-order ─
    # We extract join conditions from this raw tree so we can compute the cost
    # of the naive left-to-right plan the RBO would have produced without CBO.
    raw_tree_for_order = parser.parse(MULTI_JOIN_SQL)
    raw_table_infos, join_conditions, _ = cbo._extract_plan_components(raw_tree_for_order)
    writer_order = [t.name for t in raw_table_infos]

    # ── Full RBO + CBO pipeline ───────────────────────────────────────────────
    raw_tree   = parser.parse(MULTI_JOIN_SQL)
    rbo_tree   = rbo.optimize(raw_tree)
    cbo_result = cbo.optimize(rbo_tree)

    # ── RBO baseline cost = cost of original *writer-written* join order ──────
    rbo_cost = compute_original_cost(writer_order, join_conditions, catalog)
    cbo_cost = cbo_result.cost

    if rbo_cost == 0:
        pct_reduction = 0.0
    else:
        pct_reduction = (rbo_cost - cbo_cost) / rbo_cost * 100

    print(f"\n[Task 2] CBO vs RBO Cost Reduction  (simulated 1M–5M row tables)")
    print(f"  Tables (writer order)  : {writer_order}")
    print(f"  Cardinalities          : { {t: catalog.get_cardinality(t) for t in writer_order} }")
    print(f"  RBO naive order        : {writer_order}")
    print(f"  Best CBO order         : {cbo_result.ordering}")
    print(f"  RBO plan cost          : {rbo_cost:,}")
    print(f"  CBO plan cost          : {cbo_cost:,}")
    print(f"  Cost reduction         : {pct_reduction:.2f}%")

    return pct_reduction, rbo_cost, cbo_cost, cbo_result.ordering


if __name__ == "__main__":
    main()
