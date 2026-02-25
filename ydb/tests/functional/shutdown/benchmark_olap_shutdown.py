#!/usr/bin/env python3
"""Benchmark: OLAP query resilience during graceful node shutdown.

Measures the impact of `enable_shutting_down_node_state` feature flag
on analytical (TPC-H / column store) workloads during rolling restarts.

Data: TPC-H dataset in YDB column store tables (pre-loaded).

Phases:
  check   - Verify TPC-H data is loaded and queries work
  run     - Execute benchmark: heavy OLAP queries + node shutdown
  compare - Compare results from two benchmark runs

Usage:
    # Step 1: Verify TPC-H data is loaded
    python3 benchmark_olap_shutdown.py check \\
        --endpoint grpc://node1:2135 --database /Root

    # Step 2: Benchmark WITH feature
    # IMPORTANT: Configure feature flag in cluster YAML:
    #   feature_flags:
    #     enable_shutting_down_node_state: true
    python3 benchmark_olap_shutdown.py run \\
        --endpoint grpc://node1:2135 --database /Root \\
        --feature-flag-state on \\
        --shutdown-cmd "ssh node3 sudo systemctl stop ydbd" \\
        --restart-cmd "ssh node3 sudo systemctl start ydbd" \\
        --output results_feature_on.json --label "feature_on"

    # Step 3: Benchmark WITHOUT feature
    # IMPORTANT: Reconfigure cluster YAML:
    #   feature_flags:
    #     enable_shutting_down_node_state: false
    # Then restart cluster nodes
    python3 benchmark_olap_shutdown.py run \\
        --endpoint grpc://node1:2135 --database /Root \\
        --feature-flag-state off \\
        --shutdown-cmd "ssh node3 sudo systemctl stop ydbd" \\
        --restart-cmd "ssh node3 sudo systemctl start ydbd" \\
        --output results_feature_off.json --label "feature_off"

    # Step 4: Compare
    python3 benchmark_olap_shutdown.py compare \\
        results_feature_on.json results_feature_off.json
"""

import argparse
import json
import logging
import statistics
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

try:
    import ydb
except ImportError:
    sys.exit("ERROR: ydb package not found. Install: pip install ydb")

logger = logging.getLogger("olap_bench")


# ---------------------------------------------------------------------------
# TPC-H analytical queries adapted for YDB column store
# ---------------------------------------------------------------------------

def get_tpch_queries(path: str) -> Dict[str, str]:
    """Return heavy TPC-H queries parametrized by table path prefix.

    Selected queries cover different patterns:
      Q1  - full scan + aggregation (single table, heaviest scan)
      Q3  - 3-table JOIN + aggregation
      Q5  - 6-table JOIN + aggregation
      Q6  - scan with arithmetic filters (single table)
      Q10 - 4-table JOIN + aggregation + ORDER BY
      Q12 - 2-table JOIN + CASE expressions
      
    Heavy queries (HEAVY_*) designed for longer execution:
      HEAVY_window_analytics - multiple window functions with large windows
      HEAVY_multi_join_correlated - multiple JOINs with correlated subqueries
      HEAVY_union_all_aggregations - UNION ALL with multiple aggregations
      HEAVY_cross_join_aggregation - cross joins with complex aggregations
      HEAVY_nested_subqueries - multiple nested correlated subqueries
    """
    p = path.rstrip("/")
    return {
        "Q1_pricing_summary": f"""
            SELECT
                l_returnflag,
                l_linestatus,
                SUM(l_quantity) AS sum_qty,
                SUM(l_extendedprice) AS sum_base_price,
                SUM(l_extendedprice * (CAST(1 AS Double) - l_discount)) AS sum_disc_price,
                SUM(l_extendedprice * (CAST(1 AS Double) - l_discount)
                    * (CAST(1 AS Double) + l_tax)) AS sum_charge,
                AVG(l_quantity) AS avg_qty,
                AVG(l_extendedprice) AS avg_price,
                AVG(l_discount) AS avg_disc,
                COUNT(*) AS count_order
            FROM `{p}/lineitem`
            WHERE l_shipdate <= Date('1998-09-02')
            GROUP BY l_returnflag, l_linestatus
            ORDER BY l_returnflag, l_linestatus
        """,

        "Q3_shipping_priority": f"""
            SELECT
                l.l_orderkey,
                SUM(l.l_extendedprice * (CAST(1 AS Double) - l.l_discount)) AS revenue,
                o.o_orderdate,
                o.o_shippriority
            FROM `{p}/customer` AS c
            JOIN `{p}/orders` AS o ON c.c_custkey = o.o_custkey
            JOIN `{p}/lineitem` AS l ON l.l_orderkey = o.o_orderkey
            WHERE c.c_mktsegment = 'BUILDING'
                AND o.o_orderdate < Date('1995-03-15')
                AND l.l_shipdate > Date('1995-03-15')
            GROUP BY l.l_orderkey, o.o_orderdate, o.o_shippriority
            ORDER BY revenue DESC, o.o_orderdate
            LIMIT 10
        """,

        "Q5_local_supplier_volume": f"""
            SELECT
                n.n_name,
                SUM(l.l_extendedprice * (CAST(1 AS Double) - l.l_discount)) AS revenue
            FROM `{p}/customer` AS c
            JOIN `{p}/orders` AS o ON c.c_custkey = o.o_custkey
            JOIN `{p}/lineitem` AS l ON l.l_orderkey = o.o_orderkey
            JOIN `{p}/supplier` AS s ON l.l_suppkey = s.s_suppkey
                AND c.c_nationkey = s.s_nationkey
            JOIN `{p}/nation` AS n ON s.s_nationkey = n.n_nationkey
            JOIN `{p}/region` AS r ON n.n_regionkey = r.r_regionkey
            WHERE r.r_name = 'ASIA'
                AND o.o_orderdate >= Date('1994-01-01')
                AND o.o_orderdate < Date('1995-01-01')
            GROUP BY n.n_name
            ORDER BY revenue DESC
        """,

        "Q6_forecasting_revenue": f"""
            SELECT
                SUM(l_extendedprice * l_discount) AS revenue
            FROM `{p}/lineitem`
            WHERE l_shipdate >= Date('1994-01-01')
                AND l_shipdate < Date('1995-01-01')
                AND l_discount BETWEEN 0.05 AND 0.07
                AND l_quantity < 24
        """,

        "Q10_returned_items": f"""
            SELECT
                c.c_custkey,
                c.c_name,
                SUM(l.l_extendedprice * (CAST(1 AS Double) - l.l_discount)) AS revenue,
                c.c_acctbal,
                n.n_name,
                c.c_address,
                c.c_phone,
                c.c_comment
            FROM `{p}/customer` AS c
            JOIN `{p}/orders` AS o ON c.c_custkey = o.o_custkey
            JOIN `{p}/lineitem` AS l ON l.l_orderkey = o.o_orderkey
            JOIN `{p}/nation` AS n ON c.c_nationkey = n.n_nationkey
            WHERE o.o_orderdate >= Date('1993-10-01')
                AND o.o_orderdate < Date('1994-01-01')
                AND l.l_returnflag = 'R'
            GROUP BY c.c_custkey, c.c_name, c.c_acctbal, c.c_phone,
                     n.n_name, c.c_address, c.c_comment
            ORDER BY revenue DESC
            LIMIT 20
        """,

        "Q12_shipping_modes": f"""
            SELECT
                l.l_shipmode,
                SUM(CASE
                    WHEN o.o_orderpriority = '1-URGENT'
                        OR o.o_orderpriority = '2-HIGH'
                    THEN 1 ELSE 0 END) AS high_line_count,
                SUM(CASE
                    WHEN o.o_orderpriority <> '1-URGENT'
                        AND o.o_orderpriority <> '2-HIGH'
                    THEN 1 ELSE 0 END) AS low_line_count
            FROM `{p}/orders` AS o
            JOIN `{p}/lineitem` AS l ON o.o_orderkey = l.l_orderkey
            WHERE l.l_shipmode IN ('MAIL', 'SHIP')
                AND l.l_commitdate < l.l_receiptdate
                AND l.l_shipdate < l.l_commitdate
                AND l.l_receiptdate >= Date('1994-01-01')
                AND l.l_receiptdate < Date('1995-01-01')
            GROUP BY l.l_shipmode
            ORDER BY l.l_shipmode
        """,

        "HEAVY_window_analytics": f"""
            SELECT
                l.l_orderkey,
                l.l_partkey,
                l.l_extendedprice,
                l.l_discount,
                l.l_quantity,
                SUM(l.l_extendedprice) OVER (
                    PARTITION BY l.l_orderkey 
                    ORDER BY l.l_partkey 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cumulative_price,
                AVG(l.l_extendedprice) OVER (
                    PARTITION BY l.l_orderkey 
                    ORDER BY l.l_partkey 
                    ROWS BETWEEN 10 PRECEDING AND 10 FOLLOWING
                ) AS moving_avg_price,
                RANK() OVER (
                    PARTITION BY l.l_orderkey 
                    ORDER BY l.l_extendedprice DESC
                ) AS price_rank,
                DENSE_RANK() OVER (
                    PARTITION BY l.l_orderkey 
                    ORDER BY l.l_discount DESC
                ) AS discount_rank,
                LAG(l.l_extendedprice, 1) OVER (
                    PARTITION BY l.l_orderkey 
                    ORDER BY l.l_partkey
                ) AS prev_price,
                LEAD(l.l_extendedprice, 1) OVER (
                    PARTITION BY l.l_orderkey 
                    ORDER BY l.l_partkey
                ) AS next_price,
                PERCENT_RANK() OVER (
                    PARTITION BY l.l_orderkey 
                    ORDER BY l.l_extendedprice
                ) AS price_percent_rank,
                STDDEV(l.l_extendedprice) OVER (
                    PARTITION BY l.l_orderkey
                ) AS price_stddev,
                COUNT(*) OVER (
                    PARTITION BY l.l_orderkey
                ) AS items_in_order
            FROM `{p}/lineitem` AS l
            WHERE l.l_shipdate >= Date('1994-01-01')
                AND l.l_shipdate < Date('1995-01-01')
            ORDER BY l.l_orderkey, l.l_partkey
            LIMIT 50000
        """,

        "HEAVY_multi_join_correlated": f"""
            SELECT
                c.c_custkey,
                c.c_name,
                o.o_orderkey,
                o.o_orderdate,
                COUNT(DISTINCT l1.l_partkey) AS distinct_parts,
                COUNT(DISTINCT l2.l_suppkey) AS distinct_suppliers,
                SUM(l1.l_extendedprice * (CAST(1 AS Double) - l1.l_discount)) AS total_revenue,
                AVG(l1.l_quantity) AS avg_quantity,
                (SELECT COUNT(*) 
                 FROM `{p}/lineitem` l3 
                 WHERE l3.l_orderkey = o.o_orderkey 
                   AND l3.l_extendedprice > l1.l_extendedprice
                ) AS higher_price_items,
                (SELECT SUM(l4.l_extendedprice)
                 FROM `{p}/lineitem` l4
                 JOIN `{p}/orders` o2 ON l4.l_orderkey = o2.o_orderkey
                 WHERE o2.o_custkey = c.c_custkey
                   AND o2.o_orderdate < o.o_orderdate
                ) AS prev_orders_total
            FROM `{p}/customer` AS c
            JOIN `{p}/orders` AS o ON c.c_custkey = o.o_custkey
            JOIN `{p}/lineitem` AS l1 ON l1.l_orderkey = o.o_orderkey
            LEFT JOIN `{p}/lineitem` AS l2 ON l2.l_orderkey = o.o_orderkey
            JOIN `{p}/nation` AS n ON c.c_nationkey = n.n_nationkey
            JOIN `{p}/region` AS r ON n.n_regionkey = r.r_regionkey
            WHERE o.o_orderdate >= Date('1993-01-01')
                AND o.o_orderdate < Date('1995-01-01')
                AND r.r_name IN ('ASIA', 'EUROPE', 'AMERICA')
            GROUP BY c.c_custkey, c.c_name, o.o_orderkey, o.o_orderdate, l1.l_extendedprice
            HAVING COUNT(DISTINCT l1.l_partkey) > 5
            ORDER BY total_revenue DESC
            LIMIT 10000
        """,

        "HEAVY_union_all_aggregations": f"""
            SELECT order_year, region, SUM(revenue) AS total_revenue, COUNT(*) AS order_count
            FROM (
                SELECT 
                    CAST(YEAR(o.o_orderdate) AS Uint32) AS order_year,
                    r.r_name AS region,
                    l.l_extendedprice * (CAST(1 AS Double) - l.l_discount) AS revenue
                FROM `{p}/lineitem` AS l
                JOIN `{p}/orders` AS o ON l.l_orderkey = o.o_orderkey
                JOIN `{p}/customer` AS c ON o.o_custkey = c.c_custkey
                JOIN `{p}/nation` AS n ON c.c_nationkey = n.n_nationkey
                JOIN `{p}/region` AS r ON n.n_regionkey = r.r_regionkey
                WHERE o.o_orderdate >= Date('1992-01-01')
                    AND o.o_orderdate < Date('1996-01-01')
                
                UNION ALL
                
                SELECT 
                    CAST(YEAR(o.o_orderdate) AS Uint32) AS order_year,
                    r.r_name AS region,
                    l.l_extendedprice * l.l_discount AS revenue
                FROM `{p}/lineitem` AS l
                JOIN `{p}/orders` AS o ON l.l_orderkey = o.o_orderkey
                JOIN `{p}/supplier` AS s ON l.l_suppkey = s.s_suppkey
                JOIN `{p}/nation` AS n ON s.s_nationkey = n.n_nationkey
                JOIN `{p}/region` AS r ON n.n_regionkey = r.r_regionkey
                WHERE o.o_orderdate >= Date('1992-01-01')
                    AND o.o_orderdate < Date('1996-01-01')
                
                UNION ALL
                
                SELECT 
                    CAST(YEAR(o.o_orderdate) AS Uint32) AS order_year,
                    r.r_name AS region,
                    l.l_extendedprice * (CAST(1 AS Double) + l.l_tax) AS revenue
                FROM `{p}/lineitem` AS l
                JOIN `{p}/orders` AS o ON l.l_orderkey = o.o_orderkey
                JOIN `{p}/customer` AS c ON o.o_custkey = c.c_custkey
                JOIN `{p}/nation` AS n ON c.c_nationkey = n.n_nationkey
                JOIN `{p}/region` AS r ON n.n_regionkey = r.r_regionkey
                WHERE o.o_orderdate >= Date('1992-01-01')
                    AND o.o_orderdate < Date('1996-01-01')
            )
            GROUP BY order_year, region
            ORDER BY order_year DESC, total_revenue DESC
        """,

        "HEAVY_cross_join_aggregation": f"""
            SELECT
                p1.p_partkey AS part1,
                p2.p_partkey AS part2,
                COUNT(DISTINCT l1.l_orderkey) AS shared_orders,
                SUM(l1.l_extendedprice + l2.l_extendedprice) AS combined_revenue,
                AVG(l1.l_quantity + l2.l_quantity) AS avg_combined_quantity
            FROM `{p}/part` AS p1
            JOIN `{p}/lineitem` AS l1 ON p1.p_partkey = l1.l_partkey
            JOIN `{p}/lineitem` AS l2 ON l1.l_orderkey = l2.l_orderkey
            JOIN `{p}/part` AS p2 ON l2.l_partkey = p2.p_partkey
            JOIN `{p}/orders` AS o ON l1.l_orderkey = o.o_orderkey
            WHERE o.o_orderdate >= Date('1994-01-01')
                AND o.o_orderdate < Date('1995-01-01')
                AND p1.p_partkey < p2.p_partkey
                AND l1.l_shipdate >= Date('1994-01-01')
                AND l2.l_shipdate >= Date('1994-01-01')
            GROUP BY p1.p_partkey, p2.p_partkey
            HAVING COUNT(DISTINCT l1.l_orderkey) > 10
            ORDER BY shared_orders DESC, combined_revenue DESC
            LIMIT 5000
        """,

        "HEAVY_nested_subqueries": f"""
            SELECT
                o.o_orderkey,
                o.o_orderdate,
                o.o_totalprice,
                (SELECT COUNT(*) 
                 FROM `{p}/lineitem` l1 
                 WHERE l1.l_orderkey = o.o_orderkey
                ) AS lineitem_count,
                (SELECT SUM(l2.l_extendedprice * (CAST(1 AS Double) - l2.l_discount))
                 FROM `{p}/lineitem` l2 
                 WHERE l2.l_orderkey = o.o_orderkey
                ) AS calculated_total,
                (SELECT AVG(l3.l_quantity)
                 FROM `{p}/lineitem` l3
                 WHERE l3.l_orderkey = o.o_orderkey
                ) AS avg_quantity,
                (SELECT MAX(l4.l_extendedprice)
                 FROM `{p}/lineitem` l4
                 WHERE l4.l_orderkey = o.o_orderkey
                ) AS max_price,
                (SELECT COUNT(DISTINCT l5.l_partkey)
                 FROM `{p}/lineitem` l5
                 WHERE l5.l_orderkey = o.o_orderkey
                ) AS distinct_parts,
                (SELECT COUNT(DISTINCT l6.l_suppkey)
                 FROM `{p}/lineitem` l6
                 WHERE l6.l_orderkey = o.o_orderkey
                ) AS distinct_suppliers
            FROM `{p}/orders` AS o
            WHERE o.o_orderdate >= Date('1993-01-01')
                AND o.o_orderdate < Date('1995-01-01')
            ORDER BY o.o_orderdate DESC, o.o_totalprice DESC
            LIMIT 20000
        """,
    }


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class QueryResult:
    query_name: str
    status: str          # "ok", "unavailable", "error"
    latency_sec: float
    phase: str = ""      # "warmup", "baseline", "shutdown", "recovery"
    error_message: str = ""
    timestamp: float = 0.0


def _percentile(values: List[float], p: int) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    idx = min(int(len(s) * p / 100), len(s) - 1)
    return s[idx]


def compute_phase_stats(results: List[QueryResult]) -> Dict:
    """Aggregate per-query results into per-phase statistics."""
    phases: Dict[str, dict] = {}
    for r in results:
        if r.phase not in phases:
            phases[r.phase] = {
                "total": 0, "ok": 0, "unavailable": 0, "errors": 0,
                "_lats": [],
            }
        ps = phases[r.phase]
        ps["total"] += 1
        if r.status == "ok":
            ps["ok"] += 1
            ps["_lats"].append(r.latency_sec)
        elif r.status == "unavailable":
            ps["unavailable"] += 1
        else:
            ps["errors"] += 1

    for ps in phases.values():
        lats = ps.pop("_lats")
        ps["success_rate"] = 100.0 * ps["ok"] / ps["total"] if ps["total"] > 0 else 0.0
        ps["avg_latency"] = statistics.mean(lats) if lats else 0.0
        ps["p50_latency"] = _percentile(lats, 50)
        ps["p99_latency"] = _percentile(lats, 99)

    return phases


# ---------------------------------------------------------------------------
# Benchmark engine
# ---------------------------------------------------------------------------

class OlapBenchmark:
    def __init__(self, endpoint: str, database: str, tpch_path: str,
                 pool_size: int = 20, feature_flag_state: Optional[str] = None):
        # Normalize endpoint: remove grpc:// prefix if present
        endpoint_clean = endpoint.replace("grpc://", "").replace("http://", "")
        # If no port specified, add default grpc port
        if ":" not in endpoint_clean:
            endpoint_clean = f"{endpoint_clean}:31001"
        self.endpoint = endpoint_clean
        self.database = database
        
        # Normalize tpch_path: if it's absolute and starts with database path, make it relative
        tpch_path_clean = tpch_path.rstrip("/")
        if tpch_path_clean.startswith(database):
            # Remove database prefix to make it relative
            tpch_path_clean = tpch_path_clean[len(database):].lstrip("/")
        elif tpch_path_clean.startswith("/Root/"):
            # Remove /Root prefix
            tpch_path_clean = tpch_path_clean[len("/Root/"):]
        elif tpch_path_clean.startswith("/"):
            # Remove leading slash for absolute paths
            tpch_path_clean = tpch_path_clean.lstrip("/")
        
        self.tpch_path = tpch_path_clean
        self.pool_size = pool_size
        self.feature_flag_state = feature_flag_state
        self.driver: Optional[ydb.Driver] = None
        self.pool: Optional[ydb.QuerySessionPool] = None
        self.queries = get_tpch_queries(tpch_path_clean)

    def connect(self):
        logger.info(f"Connecting to endpoint={self.endpoint}, database={self.database}...")
        try:
            self.driver = ydb.Driver(
                ydb.DriverConfig(database=self.database, endpoint=self.endpoint)
            )
            logger.info("Waiting for driver to initialize (timeout=60s)...")
            self.driver.wait(timeout=60)
            self.pool = ydb.QuerySessionPool(self.driver, size=self.pool_size)
            logger.info(f"✓ Connected successfully: endpoint={self.endpoint}, database={self.database}")
            if self.feature_flag_state:
                logger.info(
                    f"Feature flag state: enable_shutting_down_node_state={self.feature_flag_state} "
                    f"(NOTE: Ensure this is configured in cluster YAML config)"
                )
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            logger.error(f"  Endpoint: {self.endpoint}")
            logger.error(f"  Database: {self.database}")
            logger.error("  Check:")
            logger.error("    - Endpoint is accessible and correct")
            logger.error("    - Database path exists")
            logger.error("    - Network connectivity")
            if self.driver:
                try:
                    self.driver.stop()
                except Exception:
                    pass
            raise

    def disconnect(self):
        if self.pool:
            self.pool.stop()
        if self.driver:
            self.driver.stop()

    def execute_query(self, query_name: str) -> QueryResult:
        """Execute a single TPC-H query, measure latency and status."""
        sql = self.queries[query_name]
        start = time.time()
        try:
            self.pool.execute_with_retries(sql)
            return QueryResult(
                query_name=query_name, status="ok",
                latency_sec=time.time() - start, timestamp=start,
            )
        except ydb.Unavailable as e:
            return QueryResult(
                query_name=query_name, status="unavailable",
                latency_sec=time.time() - start,
                error_message=str(e)[:300], timestamp=start,
            )
        except Exception as e:
            err = str(e)
            status = "unavailable" if "UNAVAILABLE" in err.upper() else "error"
            return QueryResult(
                query_name=query_name, status=status,
                latency_sec=time.time() - start,
                error_message=err[:300], timestamp=start,
            )

    def run_benchmark(
        self,
        concurrency: int,
        warmup_sec: int,
        baseline_sec: int,
        shutdown_sec: int,
        recovery_sec: int,
        shutdown_cmd: Optional[str],
        restart_cmd: Optional[str],
    ) -> Dict:
        """Run the full benchmark through all phases.

        Timeline:
          [warmup] -> [baseline] -> trigger shutdown -> [shutdown]
                   -> trigger restart -> [recovery]
        """
        results: List[QueryResult] = []
        lock = threading.Lock()
        stop = threading.Event()
        current_phase = {"value": "warmup"}

        def worker(worker_id: int):
            qnames = list(self.queries.keys())
            idx = worker_id
            while not stop.is_set():
                qname = qnames[idx % len(qnames)]
                idx += 1
                r = self.execute_query(qname)
                r.phase = current_phase["value"]
                with lock:
                    results.append(r)
                sym = {"ok": "+", "unavailable": "!", "error": "x"}.get(r.status, "?")
                logger.info(
                    f"  [{r.phase:>10}] {sym} {r.query_name}: "
                    f"{r.status} {r.latency_sec:.2f}s"
                )

        threads = []
        for i in range(concurrency):
            t = threading.Thread(target=worker, args=(i,), daemon=True)
            t.start()
            threads.append(t)

        t0 = time.time()

        # Phase 1: warmup (let caches/connections settle)
        logger.info(f"=== WARMUP ({warmup_sec}s) ===")
        time.sleep(warmup_sec)

        # Phase 2: baseline (steady state, no disruptions)
        current_phase["value"] = "baseline"
        logger.info(f"=== BASELINE ({baseline_sec}s) ===")
        time.sleep(baseline_sec)

        # Phase 3: shutdown
        current_phase["value"] = "shutdown"
        logger.info(f"=== SHUTDOWN ({shutdown_sec}s) ===")
        if shutdown_cmd:
            logger.info(f"  Executing: {shutdown_cmd}")
            subprocess.Popen(shutdown_cmd, shell=True)
        time.sleep(shutdown_sec)

        # Phase 4: recovery
        current_phase["value"] = "recovery"
        logger.info(f"=== RECOVERY ({recovery_sec}s) ===")
        if restart_cmd:
            logger.info(f"  Executing: {restart_cmd}")
            subprocess.Popen(restart_cmd, shell=True)
        time.sleep(recovery_sec)

        stop.set()
        for t in threads:
            t.join(timeout=60)

        elapsed = time.time() - t0
        phase_stats = compute_phase_stats(results)

        return {
            "elapsed_sec": elapsed,
            "concurrency": concurrency,
            "total_queries": len(results),
            "phase_stats": phase_stats,
            "results": [
                {
                    "query_name": r.query_name,
                    "status": r.status,
                    "latency_sec": round(r.latency_sec, 4),
                    "phase": r.phase,
                    "timestamp": round(r.timestamp, 3),
                    "error_message": r.error_message,
                }
                for r in results
            ],
        }


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

PHASE_ORDER = ["warmup", "baseline", "shutdown", "recovery"]


def print_phase_stats(phase_stats: Dict):
    header = (
        f"{'Phase':<12} {'Total':>6} {'OK':>6} {'Unavail':>8} "
        f"{'Errors':>7} {'Success%':>9} {'AvgLat':>8} {'P50':>8} {'P99':>8}"
    )
    logger.info(header)
    logger.info("-" * len(header))
    for name in PHASE_ORDER:
        ps = phase_stats.get(name)
        if not ps:
            continue
        logger.info(
            f"{name:<12} {ps['total']:>6} {ps['ok']:>6} "
            f"{ps['unavailable']:>8} {ps['errors']:>7} "
            f"{ps['success_rate']:>8.1f}% "
            f"{ps['avg_latency']:>7.2f}s "
            f"{ps['p50_latency']:>7.2f}s "
            f"{ps['p99_latency']:>7.2f}s"
        )


# ---------------------------------------------------------------------------
# Subcommands
# ---------------------------------------------------------------------------

def cmd_check(args):
    """Verify TPC-H tables exist and run a test query."""
    bench = OlapBenchmark(
        args.endpoint, args.database, args.tpch_path,
        feature_flag_state=getattr(args, "feature_flag_state", None),
    )
    bench.connect()

    tables = [
        "lineitem", "orders", "customer", "supplier",
        "nation", "region", "part", "partsupp",
    ]
    
    logger.info(f"Database: {args.database}")
    logger.info(f"TPC-H path (original): {args.tpch_path}")
    logger.info(f"TPC-H path (normalized): {bench.tpch_path}")
    logger.info(f"Checking TPC-H tables at {bench.tpch_path}/...")

    found_tables = []
    for tbl in tables:
        table_path = f"`{bench.tpch_path}/{tbl}`"
        try:
            result = bench.pool.execute_with_retries(
                f"SELECT COUNT(*) AS cnt FROM {table_path}"
            )
            cnt = result[0].rows[0]["cnt"]
            logger.info(f"  {tbl:<12}: {cnt:>12,} rows")
            found_tables.append(tbl)
        except Exception as e:
            logger.error(f"  {tbl:<12}: ERROR - {e}")
    
    if not found_tables:
        logger.error("")
        logger.error("No tables found! Please check:")
        logger.error(f"  1. Table path is correct (current: {args.tpch_path})")
        logger.error(f"  2. Database path is correct (current: {args.database})")
        logger.error(f"  3. You have SELECT permissions on tables")
        logger.error("")
        logger.error("Try running with explicit path:")
        logger.error(f"  --tpch-path /Root/testdb/tpch/s1")
        logger.error("  or")
        logger.error(f"  --tpch-path testdb/tpch/s1")
        return

    logger.info("Running test query (Q6 - single table scan)...")
    r = bench.execute_query("Q6_forecasting_revenue")
    logger.info(f"  Q6: status={r.status}, latency={r.latency_sec:.2f}s")

    logger.info("Running test query (Q5 - 6-table JOIN)...")
    r = bench.execute_query("Q5_local_supplier_volume")
    logger.info(f"  Q5: status={r.status}, latency={r.latency_sec:.2f}s")

    bench.disconnect()
    logger.info("Check complete.")


def cmd_run(args):
    """Run the benchmark: heavy OLAP queries with optional node shutdown."""
    bench = OlapBenchmark(
        args.endpoint, args.database, args.tpch_path,
        pool_size=args.concurrency * 5,
        feature_flag_state=args.feature_flag_state,
    )
    bench.connect()

    config = {
        "endpoint": args.endpoint,
        "database": args.database,
        "tpch_path": args.tpch_path,
        "concurrency": args.concurrency,
        "warmup_sec": args.warmup,
        "baseline_sec": args.baseline,
        "shutdown_sec": args.shutdown_duration,
        "recovery_sec": args.recovery,
        "shutdown_cmd": args.shutdown_cmd or "",
        "restart_cmd": args.restart_cmd or "",
        "label": args.label,
        "feature_flag_state": args.feature_flag_state or "unknown",
    }

    logger.info("=" * 72)
    logger.info(f"OLAP Shutdown Benchmark: {args.label}")
    logger.info("=" * 72)
    for k, v in config.items():
        logger.info(f"  {k}: {v}")
    logger.info("=" * 72)

    data = bench.run_benchmark(
        concurrency=args.concurrency,
        warmup_sec=args.warmup,
        baseline_sec=args.baseline,
        shutdown_sec=args.shutdown_duration,
        recovery_sec=args.recovery,
        shutdown_cmd=args.shutdown_cmd,
        restart_cmd=args.restart_cmd,
    )
    data["config"] = config
    data["label"] = args.label

    logger.info("")
    logger.info("=" * 72)
    logger.info("RESULTS:")
    logger.info("=" * 72)
    print_phase_stats(data["phase_stats"])
    logger.info(
        f"\nTotal queries: {data['total_queries']}, "
        f"elapsed: {data['elapsed_sec']:.1f}s"
    )
    logger.info("=" * 72)

    if args.output:
        with open(args.output, "w") as f:
            json.dump(data, f, indent=2)
        logger.info(f"Results saved to {args.output}")

    bench.disconnect()


def cmd_compare(args):
    """Compare two benchmark result files side by side."""
    with open(args.file1) as f:
        r1 = json.load(f)
    with open(args.file2) as f:
        r2 = json.load(f)

    label1 = r1.get("label", args.file1)
    label2 = r2.get("label", args.file2)

    logger.info("=" * 80)
    logger.info(f"COMPARISON: [{label1}] vs [{label2}]")
    logger.info("=" * 80)

    metrics = [
        ("success_rate", "%", True),
        ("avg_latency", "s", False),
        ("p50_latency", "s", False),
        ("p99_latency", "s", False),
        ("total", "", True),
        ("ok", "", True),
        ("unavailable", "", False),
        ("errors", "", False),
    ]

    header = f"{'Phase':<12} {'Metric':<14} {label1:>15} {label2:>15} {'Delta':>12}"
    logger.info(header)
    logger.info("-" * len(header))

    for phase in PHASE_ORDER:
        s1 = r1.get("phase_stats", {}).get(phase, {})
        s2 = r2.get("phase_stats", {}).get(phase, {})
        if not s1 and not s2:
            continue
        for metric, unit, higher_is_better in metrics:
            v1 = s1.get(metric, 0)
            v2 = s2.get(metric, 0)
            delta = v1 - v2

            if isinstance(v1, float):
                v1s = f"{v1:.2f}{unit}"
                v2s = f"{v2:.2f}{unit}"
                ds = f"{delta:+.2f}{unit}"
            else:
                v1s = f"{v1}{unit}"
                v2s = f"{v2}{unit}"
                ds = f"{delta:+d}{unit}"

            # Arrow indicates which direction is better
            if delta > 0:
                arrow = " ^" if higher_is_better else " v"
            elif delta < 0:
                arrow = " v" if higher_is_better else " ^"
            else:
                arrow = ""

            logger.info(
                f"{phase:<12} {metric:<14} {v1s:>15} {v2s:>15} {ds:>10}{arrow}"
            )
        logger.info("")

    logger.info("=" * 80)
    logger.info("Legend: ^ = first file is better, v = second file is better")
    logger.info("=" * 80)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="OLAP Shutdown Benchmark - measure graceful shutdown "
                    "impact on TPC-H analytical queries",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Verbose (DEBUG) output"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # --- check ---
    p_check = sub.add_parser("check", help="Verify TPC-H data is loaded")
    p_check.add_argument("--endpoint", required=True)
    p_check.add_argument("--database", required=True)
    p_check.add_argument(
        "--tpch-path", default="/Root/testdb/tpch/s1",
        help="Path prefix for TPC-H tables (default: /Root/testdb/tpch/s1)",
    )
    p_check.add_argument(
        "--feature-flag-state", choices=["on", "off"],
        help="Document feature flag state (on/off). "
             "Feature flag must be configured in cluster YAML config.",
    )

    # --- run ---
    p_run = sub.add_parser("run", help="Run the benchmark")
    p_run.add_argument("--endpoint", required=True)
    p_run.add_argument("--database", required=True)
    p_run.add_argument(
        "--tpch-path", default="/Root/testdb/tpch/s1",
        help="Path prefix for TPC-H tables (default: /Root/testdb/tpch/s1)",
    )
    p_run.add_argument(
        "--concurrency", type=int, default=4,
        help="Number of concurrent query threads (default: 4)",
    )
    p_run.add_argument(
        "--warmup", type=int, default=30,
        help="Warmup phase duration in seconds (default: 30)",
    )
    p_run.add_argument(
        "--baseline", type=int, default=30,
        help="Baseline phase duration in seconds (default: 30)",
    )
    p_run.add_argument(
        "--shutdown-duration", type=int, default=60,
        help="Shutdown phase duration in seconds (default: 60)",
    )
    p_run.add_argument(
        "--recovery", type=int, default=30,
        help="Recovery phase duration in seconds (default: 30)",
    )
    p_run.add_argument(
        "--shutdown-cmd",
        help="Shell command to stop a node, e.g. "
             "'ssh node3 sudo systemctl stop ydbd'",
    )
    p_run.add_argument(
        "--restart-cmd",
        help="Shell command to restart the node, e.g. "
             "'ssh node3 sudo systemctl start ydbd'",
    )
    p_run.add_argument(
        "--output", "-o", help="Save results to JSON file",
    )
    p_run.add_argument(
        "--label", default="run",
        help="Label for this run (e.g. 'feature_on', 'feature_off')",
    )
    p_run.add_argument(
        "--feature-flag-state", choices=["on", "off"],
        help="Document feature flag state: 'on' = enable_shutting_down_node_state=True, "
             "'off' = False. IMPORTANT: Feature flag must be configured in cluster YAML "
             "config file (feature_flags.enable_shutting_down_node_state) before running. "
             "This parameter is only for documentation/labeling.",
    )

    # --- compare ---
    p_cmp = sub.add_parser("compare", help="Compare two benchmark results")
    p_cmp.add_argument("file1", help="First results JSON")
    p_cmp.add_argument("file2", help="Second results JSON")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    commands = {
        "check": cmd_check,
        "run": cmd_run,
        "compare": cmd_compare,
    }
    commands[args.command](args)


if __name__ == "__main__":
    main()



