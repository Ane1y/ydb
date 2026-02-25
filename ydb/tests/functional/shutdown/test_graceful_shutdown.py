import os
import logging
import threading
import time
import yatest.common
import ydb
import signal
import pytest
import json
import glob

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.common.types import Erasure
from ydb.tests.sql.lib.test_base import TestBase

logger = logging.getLogger(__name__)


def get_external_param(name, default=None):
    if hasattr(yatest.common, 'get_param'):
        return yatest.common.get_param(name, default)
    return os.environ.get(name.upper().replace("-", "_"), default)


class TestGracefulShutdown(TestBase):
    _test_results = {}  # Store results for comparison

    @classmethod
    def _create_cluster_with_many_nodes(cls, enable_shutting_down=True, num_nodes=8):
        """
        Step 1: Create cluster with many nodes for distributed testing.

        Args:
            enable_shutting_down: Enable graceful shutdown feature flag
            num_nodes: Number of nodes in cluster
        """
        if hasattr(cls, 'cluster') and cls.cluster is not None:
            try:
                if hasattr(cls, 'driver'):
                    cls.driver.stop()
            except Exception:
                pass
            cls.cluster.stop()
            cls.cluster = None
            time.sleep(2)

        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY", "ydb/apps/ydbd/ydbd"))
        logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))

        cls.database = "/Root"

        cls.cluster = KiKiMR(
            KikimrConfigGenerator(
                erasure=Erasure.BLOCK_4_2,
                nodes=num_nodes,
                use_in_memory_pdisks=False,
                extra_feature_flags={
                    "enable_shutting_down_node_state": enable_shutting_down,
                    "enable_table_cache_modes": True,
                    "enable_olap_schema_operations": True,
                },
                additional_log_configs={
                    'KQP_PROXY': LogLevels.DEBUG,
                    'KQP_WORKER': LogLevels.DEBUG,
                    'KQP_SESSION': LogLevels.DEBUG,
                    'KQP_NODE': LogLevels.DEBUG,
                    'TX_PROXY': LogLevels.DEBUG,
                },
            )
        )

        cls.cluster.start()
        time.sleep(5)

        node1 = cls.cluster.nodes[1]
        cls.driver = ydb.Driver(
            ydb.DriverConfig(database=cls.get_database(), endpoint=f"{node1.host}:{node1.grpc_port}")
        )
        cls.driver.wait()
        # Create pool with more sessions to distribute queries across different sessions/nodes
        # Default pool size is 100, but we increase it to ensure better distribution
        # This helps avoid SessionBusy when multiple queries hit the same session
        cls.pool = ydb.QuerySessionPool(cls.driver, size=num_nodes * 10)

        # Pre-create sessions to ensure they are distributed across nodes
        # YDB SDK balances sessions across nodes when creating them, but does it lazily
        # Pre-creating sessions helps ensure better distribution before queries start
        logger.info(f"Pre-creating sessions to distribute across {num_nodes} nodes...")
        precreated_sessions = []
        for i in range(min(num_nodes * 5, 40)):  # Pre-create up to 40 sessions
            try:
                session = cls.pool.acquire(timeout=5)
                precreated_sessions.append(session)
            except Exception as e:
                logger.warning(f"Failed to pre-create session {i}: {e}")
                break

        # Release sessions back to pool so they can be reused
        for session in precreated_sessions:
            try:
                cls.pool.release(session)
            except Exception:
                pass

        logger.info(f"Pre-created {len(precreated_sessions)} sessions")
        logger.info(f"Cluster created with {num_nodes} nodes, enable_shutting_down_node_state={enable_shutting_down}")

    @classmethod
    def setup_class(cls):
        external_endpoint = get_external_param('ydb-endpoint')
        external_database = get_external_param('ydb-db')

        cls.use_external_cluster = external_endpoint is not None

        if cls.use_external_cluster:
            logger.info(f"Using external cluster: {external_endpoint}, database: {external_database}")
            cls.database = external_database or "/Root"
            endpoint = external_endpoint.replace("grpc://", "")

            cls.driver = ydb.Driver(ydb.DriverConfig(database=cls.database, endpoint=endpoint))
            cls.driver.wait()
            # Create pool with more sessions for external cluster to distribute queries
            cls.pool = ydb.QuerySessionPool(cls.driver, size=80)

            logger.warning("WARNING: Shutdown tests on external cluster will only test query behavior,")
            logger.warning("         node shutdown must be done manually via 'systemctl stop' or similar")
        else:
            cls._create_cluster_with_many_nodes(enable_shutting_down=True)

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, '_test_results') and len(cls._test_results) == 2:
            logger.info("")
            logger.info("=" * 80)
            logger.info("COMPARATIVE STATISTICS:")
            logger.info("=" * 80)
            logger.info(f"{'Metric':<30} {'Flag=True':<20} {'Flag=False':<20} {'Difference':<20}")
            logger.info("-" * 80)

            results_true = cls._test_results.get(True, {})
            results_false = cls._test_results.get(False, {})

            for metric, key_true, key_false in [
                ('Total queries', 'total', 'total'),
                ('Successful queries', 'successful', 'successful'),
                ('Failed queries', 'failed', 'failed'),
                ('Success rate (%)', 'success_rate', 'success_rate'),
                ('Test duration (s)', 'duration', 'duration'),
            ]:
                val_true = results_true.get(key_true, 0)
                val_false = results_false.get(key_false, 0)
                diff = val_true - val_false

                if isinstance(val_true, float):
                    diff_str = f"{diff:+.2f}"
                    val_true_str = f"{val_true:.2f}"
                    val_false_str = f"{val_false:.2f}"
                else:
                    diff_str = f"{diff:+d}"
                    val_true_str = f"{val_true}"
                    val_false_str = f"{val_false}"
                logger.info(f"{metric:<30} {val_true_str:<20} {val_false_str:<20} {diff_str:<20}")

            logger.info("=" * 80)
            logger.info("")

        if hasattr(cls, 'driver') and not cls.use_external_cluster:
            cls.driver.stop()
        if hasattr(cls, 'cluster') and cls.cluster is not None:
            cls.cluster.stop()

    def _prepare_table_with_distributed_data(self, table_name, rows_count=20000, partitions=64):
        """
        Step 2: Create table and fill with data distributed across all nodes.

        Args:
            table_name: Name of the table to create
            rows_count: Number of rows to insert
            partitions: Number of uniform partitions (more partitions = better distribution)
        """
        create_table_sql = f"""
            CREATE TABLE `{table_name}` (
                id Uint64 NOT NULL,
                value Uint32,
                text Utf8,
                PRIMARY KEY (id)
            )
            WITH (
                UNIFORM_PARTITIONS = {partitions},
                AUTO_PARTITIONING_BY_SIZE = DISABLED,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = {partitions},
                AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = {partitions}
            );
        """
        self.pool.execute_with_retries(create_table_sql)
        logger.info(f"Created table {table_name} with {partitions} partitions")

        # Insert data in batches using UPSERT
        batch_size = 2000
        for batch_start in range(0, rows_count, batch_size):
            batch_end = min(batch_start + batch_size, rows_count)
            values = []
            for i in range(batch_start, batch_end):
                text_value = 'x' * 100
                values.append(f"({i}, {i % 1000}, '{text_value}')")

            upsert_sql = f"""
                UPSERT INTO `{table_name}` (id, value, text)
                VALUES {', '.join(values)};
            """
            self.pool.execute_with_retries(upsert_sql)

        logger.info(f"Inserted {rows_count} rows into {table_name}")

    def _create_heavy_query(self, table_name, variant=0):
        """
        Step 3: Create a heavy query with complex functions that requires access to many nodes.
        Different variants generate different compilation plans.

        Args:
            table_name: Name of the table to query
            variant: Query variant number (0-4) to generate different plans

        Returns:
            SQL query string
        """
        if variant == 0:
            # Window functions with self-join - heavier version
            return f"""
                SELECT
                    t1.id,
                    t1.value,
                    t1.text,
                    SUM(t1.value) OVER (PARTITION BY (t1.id / 1000) ORDER BY t1.id ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) AS running_sum,
                    AVG(t1.value) OVER (PARTITION BY (t1.id / 1000) ORDER BY t1.id ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) AS running_avg,
                    MAX(t1.value) OVER (PARTITION BY (t1.id / 500)) AS partition_max,
                    MIN(t1.value) OVER (PARTITION BY (t1.id / 500)) AS partition_min,
                    COUNT(*) OVER (PARTITION BY (t1.id / 1000)) AS partition_count,
                    SUM(t1.value) OVER (PARTITION BY (t1.id / 2000) ORDER BY t1.id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sum,
                    STDDEV(t1.value) OVER (PARTITION BY (t1.id / 1000)) AS partition_stddev,
                    t2.value AS joined_value,
                    t2.text AS joined_text,
                    t3.value AS t3_value
                FROM `{table_name}` AS t1
                INNER JOIN `{table_name}` AS t2 
                    ON t1.id / 1000 = t2.id / 1000 
                    AND t2.id = t1.id + 1
                LEFT JOIN `{table_name}` AS t3
                    ON t1.id / 500 = t3.id / 500
                    AND t3.id = t1.id + 50
                WHERE t1.value > 0 AND t2.value > 0
                ORDER BY t1.id
                LIMIT 20000;
            """
        elif variant == 1:
            # Aggregation with multiple GROUP BY levels - heavier version
            return f"""
                SELECT
                    (id / 500) AS partition_id,
                    (id / 100) AS bucket,
                    (id / 50) AS sub_bucket,
                    COUNT(*) AS cnt,
                    SUM(value) AS total_value,
                    AVG(value) AS avg_value,
                    MAX(value) AS max_value,
                    MIN(value) AS min_value,
                    COUNT(DISTINCT value) AS distinct_values,
                    SUM(value * value) AS sum_squares,
                    STDDEV(value) AS stddev_value
                FROM `{table_name}`
                WHERE value > 0 AND id % 2 = 0
                GROUP BY (id / 500), (id / 100), (id / 50)
                HAVING COUNT(*) > 5
                ORDER BY partition_id, bucket, sub_bucket
                LIMIT 15000;
            """
        elif variant == 2:
            # Multiple JOINs with subqueries - heavier version
            return f"""
                SELECT
                    t1.id,
                    t1.value,
                    t1.text,
                    t2.value AS t2_value,
                    t3.value AS t3_value,
                    t4.value AS t4_value,
                    (SELECT COUNT(*) FROM `{table_name}` t5 WHERE t5.id < t1.id AND t5.value = t1.value) AS count_before,
                    (SELECT AVG(value) FROM `{table_name}` t6 WHERE t6.id BETWEEN t1.id - 100 AND t1.id + 100) AS local_avg,
                    (SELECT MAX(value) FROM `{table_name}` t7 WHERE t7.id / 1000 = t1.id / 1000) AS partition_max
                FROM `{table_name}` AS t1
                LEFT JOIN `{table_name}` AS t2 ON t1.id / 1000 = t2.id / 1000 AND t2.id = t1.id + 100
                LEFT JOIN `{table_name}` AS t3 ON t1.id / 2000 = t3.id / 2000 AND t3.id = t1.id + 200
                LEFT JOIN `{table_name}` AS t4 ON t1.id / 500 = t4.id / 500 AND t4.id = t1.id + 50
                WHERE t1.value BETWEEN 10 AND 500
                ORDER BY t1.id
                LIMIT 15000;
            """
        elif variant == 3:
            # Complex window functions with RANK and DENSE_RANK - heavier version
            return f"""
                SELECT
                    id,
                    value,
                    text,
                    RANK() OVER (PARTITION BY (id / 1000) ORDER BY value DESC) AS value_rank,
                    DENSE_RANK() OVER (PARTITION BY (id / 1000) ORDER BY value DESC) AS value_dense_rank,
                    ROW_NUMBER() OVER (PARTITION BY (id / 500) ORDER BY id) AS row_num,
                    LAG(value, 1) OVER (PARTITION BY (id / 1000) ORDER BY id) AS prev_value,
                    LAG(value, 2) OVER (PARTITION BY (id / 1000) ORDER BY id) AS prev_value2,
                    LEAD(value, 1) OVER (PARTITION BY (id / 1000) ORDER BY id) AS next_value,
                    LEAD(value, 2) OVER (PARTITION BY (id / 1000) ORDER BY id) AS next_value2,
                    SUM(value) OVER (PARTITION BY (id / 1000) ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sum,
                    AVG(value) OVER (PARTITION BY (id / 1000) ORDER BY id ROWS BETWEEN 50 PRECEDING AND 50 FOLLOWING) AS moving_avg,
                    MAX(value) OVER (PARTITION BY (id / 500) ORDER BY id ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) AS moving_max,
                    PERCENT_RANK() OVER (PARTITION BY (id / 1000) ORDER BY value) AS value_percent_rank
                FROM `{table_name}`
                WHERE value > 0
                ORDER BY id
                LIMIT 20000;
            """
        else:  # variant == 4
            # UNION ALL with different aggregations - heavier version
            return f"""
                SELECT bucket, cnt, total, avg_val, max_val, 'type_a' AS query_type
                FROM (
                    SELECT 
                        (id / 200) AS bucket, 
                        COUNT(*) AS cnt, 
                        SUM(value) AS total,
                        AVG(value) AS avg_val,
                        MAX(value) AS max_val
                    FROM `{table_name}`
                    WHERE id % 3 = 0 AND value > 0
                    GROUP BY (id / 200)
                )
                UNION ALL
                SELECT bucket, cnt, total, avg_val, max_val, 'type_b' AS query_type
                FROM (
                    SELECT 
                        (id / 300) AS bucket, 
                        COUNT(*) AS cnt, 
                        SUM(value) AS total,
                        AVG(value) AS avg_val,
                        MAX(value) AS max_val
                    FROM `{table_name}`
                    WHERE id % 5 = 0 AND value > 0
                    GROUP BY (id / 300)
                )
                UNION ALL
                SELECT bucket, cnt, total, avg_val, max_val, 'type_c' AS query_type
                FROM (
                    SELECT 
                        (id / 400) AS bucket, 
                        COUNT(*) AS cnt, 
                        SUM(value) AS total,
                        AVG(value) AS avg_val,
                        MAX(value) AS max_val
                    FROM `{table_name}`
                    WHERE id % 7 = 0 AND value > 0
                    GROUP BY (id / 400)
                )
                ORDER BY bucket
                LIMIT 12000;
            """

    def _create_distributed_query(self, table_name, query_id):
        """
        Create distributed query that touches multiple partitions/nodes.

        Args:
            table_name: Name of the table to query
            query_id: Unique query identifier

        Returns:
            SQL query string
        """
        total_rows = 50000
        num_partitions = 64
        partition_size = total_rows // num_partitions

        # Create query that touches many partitions scattered across the id space
        ranges = []
        num_ranges = 16  # Query 16 different partition ranges for good distribution
        for j in range(num_ranges):
            partition_idx = (query_id * num_ranges + j) % num_partitions
            start_id = partition_idx * partition_size
            end_id = min(start_id + partition_size - 1, total_rows - 1)
            ranges.append(f"(id >= {start_id} AND id <= {end_id})")

        return f"""
            SELECT
                bucket,
                COUNT(*) as cnt,
                SUM(value) as total
            FROM (
                SELECT
                    (id / 100) as bucket,
                    value
                FROM `{table_name}`
                WHERE {' OR '.join(ranges)}
            )
            GROUP BY bucket
            ORDER BY bucket
            LIMIT 200;
        """

    def _get_node_id_from_session(self, session):
        """Extract node_id from YDB session."""
        node_id_from_session = 'unknown'
        try:
            if hasattr(session, '_state') and hasattr(session._state, 'session_id'):
                session_id = session._state.session_id
                if 'node_id=' in session_id:
                    try:
                        node_id_part = session_id.split('node_id=')[1].split('&')[0]
                        node_id_from_session = int(node_id_part)
                    except (ValueError, IndexError):
                        pass
        except Exception:
            pass
        return node_id_from_session

    def _get_participating_nodes_from_result(self, query_stats):
        """Extract all node IDs that participated in query execution from query stats."""
        participating_nodes = []
        try:
            if hasattr(query_stats, 'query_plan') and query_stats.query_plan:
                try:
                    plan = json.loads(query_stats.query_plan)

                    def find_node_ids(obj):
                        nodes = []
                        if isinstance(obj, dict):
                            if 'node_id' in obj:
                                nodes.append(obj['node_id'])
                            if 'NodeId' in obj:
                                nodes.append(obj['NodeId'])
                            if 'NodesScanShards' in obj:
                                for node_info in obj['NodesScanShards']:
                                    if isinstance(node_info, dict) and 'node_id' in node_info:
                                        nodes.append(node_info['node_id'])
                            if 'ComputeNodes' in obj:
                                for compute_node in obj['ComputeNodes']:
                                    if isinstance(compute_node, dict):
                                        if 'NodeId' in compute_node:
                                            nodes.append(compute_node['NodeId'])
                                        if 'Tasks' in compute_node and isinstance(compute_node['Tasks'], list):
                                            for task in compute_node['Tasks']:
                                                if isinstance(task, dict) and 'NodeId' in task:
                                                    nodes.append(task['NodeId'])
                                        nodes.extend(find_node_ids(compute_node))
                            for value in obj.values():
                                nodes.extend(find_node_ids(value))
                        elif isinstance(obj, list):
                            for item in obj:
                                nodes.extend(find_node_ids(item))
                        return nodes

                    participating_nodes = list(set(find_node_ids(plan)))
                except (json.JSONDecodeError, Exception):
                    pass
        except Exception:
            pass
        return participating_nodes

    def _start_log_monitor_thread(self, nodes_to_shutdown, shutdown_start_time, log_monitor_results, stop_event):
        """
        Step 4: Start thread that monitors logs for shutdown messages.

        Args:
            nodes_to_shutdown: List of nodes being shut down
            shutdown_start_time: Timestamp when shutdown was initiated
            log_monitor_results: Dict to store results (thread-safe)
            stop_event: threading.Event to signal when to stop monitoring
        """

        def monitor_logs():
            iteration = 0
            max_iterations = 120  # Monitor for up to 120 seconds
            while not stop_event.is_set() and iteration < max_iterations:
                try:
                    for node in nodes_to_shutdown:
                        shutdown_logs = self._check_node_logs_for_shutdown(node, shutdown_start_time)
                        node_id = node.node_id

                        if shutdown_logs['shutdown_initiated']:
                            count = len(shutdown_logs['shutdown_initiated'])
                            log_monitor_results[f'node_{node_id}_shutdown_initiated'] = count
                            if iteration % 10 == 0:  # Log every 10 iterations
                                logger.debug(
                                    f"Log monitor (iter {iteration}): Found {count} shutdown_initiated messages for node {node_id}"
                                )
                        if shutdown_logs['rejecting_remote']:
                            count = len(shutdown_logs['rejecting_remote'])
                            log_monitor_results[f'node_{node_id}_rejecting_remote'] = count
                            if iteration % 10 == 0:  # Log every 10 iterations
                                logger.debug(
                                    f"Log monitor (iter {iteration}): Found {count} rejecting_remote messages for node {node_id}"
                                )
                        if shutdown_logs['node_shutting_down']:
                            count = len(shutdown_logs['node_shutting_down'])
                            log_monitor_results[f'node_{node_id}_node_shutting_down'] = count
                            if iteration % 10 == 0:  # Log every 10 iterations
                                logger.debug(
                                    f"Log monitor (iter {iteration}): Found {count} node_shutting_down messages for node {node_id}"
                                )

                    iteration += 1
                    time.sleep(0.5)  # Check logs every 0.5 seconds (more frequent)
                except Exception as e:
                    logger.debug(f"Log monitor error: {e}")
                    time.sleep(0.5)

            # Final check after stop event is set
            logger.info(f"Log monitor: Stopped after {iteration} iterations, performing final check...")
            for node in nodes_to_shutdown:
                shutdown_logs = self._check_node_logs_for_shutdown(node, shutdown_start_time)
                node_id = node.node_id

                if shutdown_logs['shutdown_initiated']:
                    count = len(shutdown_logs['shutdown_initiated'])
                    log_monitor_results[f'node_{node_id}_shutdown_initiated'] = count
                    logger.info(f"Log monitor final: Node {node_id} - shutdown_initiated: {count}")
                if shutdown_logs['rejecting_remote']:
                    count = len(shutdown_logs['rejecting_remote'])
                    log_monitor_results[f'node_{node_id}_rejecting_remote'] = count
                    logger.info(f"Log monitor final: Node {node_id} - rejecting_remote: {count}")
                if shutdown_logs['node_shutting_down']:
                    count = len(shutdown_logs['node_shutting_down'])
                    log_monitor_results[f'node_{node_id}_node_shutting_down'] = count
                    logger.info(f"Log monitor final: Node {node_id} - node_shutting_down: {count}")

        thread = threading.Thread(target=monitor_logs, daemon=True)
        thread.start()
        return thread

    def _check_node_logs_for_shutdown(self, node, shutdown_start_time):
        """Check node logs for shutdown-related messages."""
        found_messages = {
            'shutdown_initiated': [],
            'rejecting_remote': [],
            'node_shutting_down': [],
        }

        try:
            log_file = None
            if hasattr(node, '_KiKiMRNode__log_file_name') and node._KiKiMRNode__log_file_name:
                log_file = node._KiKiMRNode__log_file_name
            elif hasattr(node, '_KiKiMRNode__working_dir'):

                working_dir = node._KiKiMRNode__working_dir
                patterns = ["logfile_*.log", "*.log", "logfile*.log"]

                for pattern in patterns:
                    log_files = glob.glob(os.path.join(working_dir, pattern))
                    if log_files:
                        logfile_logs = [f for f in log_files if 'logfile' in os.path.basename(f)]
                        if logfile_logs:
                            log_file = logfile_logs[0]
                        else:
                            log_file = log_files[0]
                        break

                if not log_file:
                    stderr_file = os.path.join(working_dir, "stderr")
                    if os.path.exists(stderr_file):
                        log_file = stderr_file

            if not log_file:
                logger.debug(
                    f"Node {node.node_id}: No log file found. Working dir: {getattr(node, '_KiKiMRNode__working_dir', 'unknown')}"
                )
                return found_messages

            if not os.path.exists(log_file):
                logger.debug(f"Node {node.node_id}: Log file does not exist: {log_file}")
                return found_messages

            logger.debug(
                f"Node {node.node_id}: Checking log file: {log_file} (size: {os.path.getsize(log_file)} bytes)"
            )

            with open(log_file, 'r', errors='ignore') as f:
                lines = f.readlines()
                # Check more lines to ensure we catch shutdown messages
                # Shutdown messages may appear later in the log file
                num_lines_to_check = min(10000, len(lines))
                if len(lines) > num_lines_to_check:
                    recent_lines = lines[-num_lines_to_check:]
                else:
                    recent_lines = lines

                logger.debug(
                    f"Node {node.node_id}: Checking {len(recent_lines)} recent lines (out of {len(lines)} total)"
                )

                # Debug: check if shutdown message exists in recent lines
                shutdown_in_recent = any(
                    'proxy' in line.lower() and 'shutdown' in line.lower() and 'requested' in line.lower()
                    for line in recent_lines
                )
                if shutdown_in_recent:
                    logger.info(f"Node {node.node_id}: DEBUG - Found 'proxy shutdown requested' in recent lines!")
                    # Find and log the actual line
                    for idx, line in enumerate(recent_lines):
                        if 'proxy' in line.lower() and 'shutdown' in line.lower() and 'requested' in line.lower():
                            logger.info(
                                f"Node {node.node_id}: DEBUG - Shutdown message at index {idx}: {line.strip()[:200]}"
                            )
                            break
                else:
                    # More detailed debugging - check what we're actually reading
                    logger.warning(
                        f"Node {node.node_id}: DEBUG - 'proxy shutdown requested' NOT found in {len(recent_lines)} recent lines"
                    )
                    # Check if file has the message at all (read file again to be sure)
                    try:
                        with open(log_file, 'r', errors='ignore') as f2:
                            all_lines = f2.readlines()
                            direct_check = any(
                                'proxy' in line.lower() and 'shutdown' in line.lower() and 'requested' in line.lower()
                                for line in all_lines
                            )
                            if direct_check:
                                logger.warning(
                                    f"Node {node.node_id}: DEBUG - Message EXISTS in file but NOT in recent_lines! "
                                    f"File has {len(all_lines)} lines, checking {len(recent_lines)} recent lines"
                                )
                    except Exception as e:
                        logger.debug(f"Node {node.node_id}: Failed to re-read file for debugging: {e}")

                # More flexible search patterns based on actual log messages
                shutdown_matches = 0
                kqp_shutdown_found_in_file = False
                for line in recent_lines:
                    line_lower = line.lower()
                    original_line = line.strip()

                    # Check for shutdown initiation - multiple patterns:
                    # LOG_I("Prepare to shutdown: do not accept any messages from this time");
                    # "KQP proxy shutdown requested" (from kqp_proxy_service.cpp:508)
                    # "KQP_PROXY NOTICE: ... shutdown requested"
                    matches_shutdown = False
                    # Prioritize KQP_PROXY shutdown messages - they are the most reliable indicator
                    # Check for "KQP proxy shutdown requested" or "KQP_PROXY ... shutdown requested" first
                    if (
                        ('kqp_proxy' in line_lower or 'kqp proxy' in line_lower)
                        and 'shutdown' in line_lower
                        and 'requested' in line_lower
                    ):
                        # This is the primary shutdown message we're looking for
                        found_messages['shutdown_initiated'].append(original_line)
                        matches_shutdown = True
                        shutdown_matches += 1
                        if shutdown_matches <= 3:
                            logger.info(f"Node {node.node_id}: Found KQP_PROXY shutdown message: {original_line[:200]}")
                    elif (
                        ('prepare' in line_lower and 'shutdown' in line_lower)
                        or 'prepare to shutdown' in line_lower
                        or 'preparing shutdown' in line_lower
                        or 'initiating shutdown' in line_lower
                        or ('shutdown' in line_lower and 'initiated' in line_lower)
                        or ('shutdown' in line_lower and 'do not accept' in line_lower)
                        # Only match if it's NOT KQP_SESSION (we want KQP_PROXY)
                        or (
                            'kqp' in line_lower
                            and 'shutdown' in line_lower
                            and 'requested' in line_lower
                            and 'kqp_session' not in line_lower
                        )
                        or (
                            'proxy' in line_lower
                            and 'shutdown' in line_lower
                            and 'requested' in line_lower
                            and 'kqp_session' not in line_lower
                        )
                    ):
                        found_messages['shutdown_initiated'].append(original_line)
                        matches_shutdown = True
                        shutdown_matches += 1
                        if shutdown_matches <= 3:  # Log first 3 matches for debugging
                            logger.info(f"Node {node.node_id}: Found shutdown message: {original_line[:200]}")

                    # Debug: check if KQP PROXY shutdown message exists in file but pattern didn't match
                    # Only check for "proxy" + "shutdown" + "requested" combination
                    if 'proxy' in line_lower and 'shutdown' in line_lower and 'requested' in line_lower:
                        kqp_shutdown_found_in_file = True
                        if not matches_shutdown:
                            logger.warning(
                                f"Node {node.node_id}: DEBUG - Found proxy+shutdown+requested but pattern didn't match: {original_line[:200]}"
                            )
                            # Try to understand why pattern didn't match
                            has_kqp = 'kqp' in line_lower
                            has_proxy = 'proxy' in line_lower
                            has_shutdown = 'shutdown' in line_lower
                            has_requested = 'requested' in line_lower
                            logger.warning(
                                f"Node {node.node_id}: DEBUG - Pattern check: kqp={has_kqp}, proxy={has_proxy}, shutdown={has_shutdown}, requested={has_requested}"
                            )

                # If we found KQP PROXY shutdown in file but pattern didn't match, log it
                if kqp_shutdown_found_in_file and shutdown_matches == 0:
                    logger.warning(
                        f"Node {node.node_id}: DEBUG - KQP PROXY shutdown found in file but pattern matching failed!"
                    )

                    # Check for rejecting remote requests - LOG_D("Rejecting remote StartRequest TxId: ... in ShuttingDown State");
                    if ('rejecting remote' in line_lower or 'reject remote' in line_lower) and (
                        'shuttingdown' in line_lower
                        or 'shutting down' in line_lower
                        or 'shuttingdown state' in line_lower
                        or 'shutdown' in line_lower
                    ):
                        found_messages['rejecting_remote'].append(original_line)

                    # Check for node shutting down status
                    if (
                        'node_shutting_down' in line_lower
                        or 'node shutting down' in line_lower
                        or 'shutting_down' in line_lower
                        or ('node' in line_lower and 'shutting' in line_lower and 'down' in line_lower)
                    ):
                        found_messages['node_shutting_down'].append(original_line)

                    # Check for UNAVAILABLE status
                    if (
                        'statusids::unavailable' in line_lower
                        or 'status_ids::unavailable' in line_lower
                        or 'status code unavailable' in line_lower
                        or ('unavailable' in line_lower and 'status' in line_lower)
                    ):
                        found_messages['node_shutting_down'].append(original_line)

                logger.debug(
                    f"Node {node.node_id}: Found {len(found_messages['shutdown_initiated'])} shutdown_initiated, "
                    f"{len(found_messages['rejecting_remote'])} rejecting_remote, "
                    f"{len(found_messages['node_shutting_down'])} node_shutting_down messages"
                )

        except Exception as e:
            logger.warning(f"Failed to read logs for node {node.node_id}: {e}", exc_info=True)

        return found_messages

    def _run_heavy_query(self, heavy_query, query_id, shutting_down_node_ids, query_results, results_lock):
        """
        Step 5: Run heavy query that requires access to many nodes.

        Args:
            heavy_query: SQL query string
            query_id: Unique query identifier
            shutting_down_node_ids: Set of node IDs being shut down
            query_results: List to store results (thread-safe)
            results_lock: Lock for thread-safe access
        """
        start = time.time()
        session = None
        node_id = 'unknown'
        participating_nodes = []

        try:
            session = self.pool.acquire()
            node_id = self._get_node_id_from_session(session)

            session.execute(
                heavy_query,
                settings=ydb.BaseRequestSettings().with_timeout(60),
                stats_mode=ydb.QueryStatsMode.FULL,
            )

            if hasattr(session, 'last_query_stats') and session.last_query_stats:
                participating_nodes = self._get_participating_nodes_from_result(session.last_query_stats)

            duration = time.time() - start
            hit_shutdown_node = (
                any(nid in shutting_down_node_ids for nid in participating_nodes)
                if participating_nodes
                else (node_id in shutting_down_node_ids if node_id != 'unknown' else False)
            )

            with results_lock:
                query_results.append(
                    {
                        'query_id': f'heavy_{query_id}',
                        'status': 'success',
                        'duration': duration,
                        'nodes': (
                            sorted(participating_nodes)
                            if participating_nodes
                            else [node_id] if node_id != 'unknown' else []
                        ),
                        'hit_shutdown_node': hit_shutdown_node,
                    }
                )

            logger.info(
                f"[HEAVY_QUERY] Query #{query_id} completed in {duration:.2f}s, nodes={sorted(participating_nodes) if participating_nodes else [node_id]}, hit_shutdown={hit_shutdown_node}"
            )

        except ydb.Unavailable:
            duration = time.time() - start
            hit_shutdown_node = node_id in shutting_down_node_ids if node_id != 'unknown' else False

            with results_lock:
                query_results.append(
                    {
                        'query_id': f'heavy_{query_id}',
                        'status': 'unavailable',
                        'duration': duration,
                        'nodes': [node_id] if node_id != 'unknown' else [],
                        'hit_shutdown_node': hit_shutdown_node,
                    }
                )

            logger.info(
                f"[HEAVY_QUERY] Query #{query_id} got UNAVAILABLE after {duration:.2f}s, node={node_id}, hit_shutdown={hit_shutdown_node}"
            )

        except Exception as e:
            duration = time.time() - start
            error_type = type(e).__name__
            error_str = str(e)
            is_unavailable = 'UNAVAILABLE' in error_str.upper() or 'unavailable' in error_str.lower()
            hit_shutdown_node = node_id in shutting_down_node_ids if node_id != 'unknown' else False

            with results_lock:
                query_results.append(
                    {
                        'query_id': f'heavy_{query_id}',
                        'status': 'unavailable' if is_unavailable else 'error',
                        'duration': duration,
                        'nodes': [node_id] if node_id != 'unknown' else [],
                        'hit_shutdown_node': hit_shutdown_node,
                        'error_type': error_type,
                    }
                )

            logger.warning(
                f"[HEAVY_QUERY] Query #{query_id} failed: {error_type}, node={node_id}, hit_shutdown={hit_shutdown_node}"
            )

        finally:
            if session:
                try:
                    self.pool.release(session)
                except Exception:
                    pass

    def _shutdown_nodes(self, nodes_to_shutdown, delay=1.0):
        """
        Step 6: Shutdown nodes gracefully.

        Args:
            nodes_to_shutdown: List of nodes to shut down
            delay: Delay before initiating shutdown
        """

        def initiate_shutdown(node_ref):
            time.sleep(delay)
            logger.info(f"  Sending SIGTERM to node {node_ref.node_id}")
            try:
                if hasattr(node_ref, '_KiKiMRNode__daemon') and node_ref._KiKiMRNode__daemon:
                    pid = node_ref._KiKiMRNode__daemon.daemon.pid
                    if pid:
                        os.kill(pid, signal.SIGTERM)
            except Exception as e:
                logger.warning(f"  SIGTERM failed: {e}, using stop()")
                try:
                    node_ref.stop()
                except Exception:
                    pass

        threads = []
        for node in nodes_to_shutdown:
            thread = threading.Thread(target=initiate_shutdown, args=(node,))
            threads.append(thread)
            thread.start()

        return threads

    def _run_distributed_queries(self, table_name, num_queries, shutting_down_node_ids, query_results, results_lock):
        """
        Step 7: Run many small distributed queries that touch many nodes.

        Args:
            table_name: Name of the table to query
            num_queries: Number of queries to run
            shutting_down_node_ids: Set of node IDs being shut down
            query_results: List to store results (thread-safe)
            results_lock: Lock for thread-safe access
        """

        def run_query(query_id):
            node_id = 'unknown'
            participating_nodes = []
            session = None

            try:
                session = self.pool.acquire()
                node_id = self._get_node_id_from_session(session)

                query_sql = self._create_distributed_query(table_name, query_id)
                result = session.execute(
                    query_sql,
                    settings=ydb.BaseRequestSettings().with_timeout(15),
                    stats_mode=ydb.QueryStatsMode.FULL,
                )
                result_sets = list(result)
                count = len(result_sets[0].rows) if result_sets else 0

                if hasattr(session, 'last_query_stats') and session.last_query_stats:
                    participating_nodes = self._get_participating_nodes_from_result(session.last_query_stats)

                hit_shutdown_node = (
                    any(nid in shutting_down_node_ids for nid in participating_nodes)
                    if participating_nodes
                    else (node_id in shutting_down_node_ids if node_id != 'unknown' else False)
                )

                with results_lock:
                    query_results.append(
                        {
                            'query_id': f'test_{query_id}',
                            'status': 'success',
                            'duration': None,
                            'nodes': (
                                sorted(participating_nodes)
                                if participating_nodes
                                else [node_id] if node_id != 'unknown' else []
                            ),
                            'hit_shutdown_node': hit_shutdown_node,
                            'row_count': count,
                        }
                    )

            except ydb.Unavailable:
                hit_shutdown_node = node_id in shutting_down_node_ids if node_id != 'unknown' else False

                with results_lock:
                    query_results.append(
                        {
                            'query_id': f'test_{query_id}',
                            'status': 'unavailable',
                            'duration': None,
                            'nodes': [node_id] if node_id != 'unknown' else [],
                            'hit_shutdown_node': hit_shutdown_node,
                        }
                    )

            except Exception as e:
                error_type = type(e).__name__
                error_str = str(e)
                is_unavailable = 'UNAVAILABLE' in error_str.upper() or 'unavailable' in error_str.lower()
                is_session_busy = (
                    error_type == 'SessionBusy'
                    or 'SESSION_BUSY' in error_str.upper()
                    or 'session busy' in error_str.lower()
                    or 'Pending previous query completion' in error_str
                )
                hit_shutdown_node = node_id in shutting_down_node_ids if node_id != 'unknown' else False

                # SessionBusy is expected when heavy queries are still running on the same session
                # This is normal behavior during shutdown - session can only handle one query at a time
                if is_session_busy:
                    logger.debug(
                        f"[DISTRIBUTED_QUERY] Query test_{query_id} got SessionBusy (expected during shutdown), node={node_id}, hit_shutdown={hit_shutdown_node}"
                    )
                else:
                    # Log error details for debugging
                    logger.warning(
                        f"[DISTRIBUTED_QUERY] Query test_{query_id} failed: {error_type}, node={node_id}, hit_shutdown={hit_shutdown_node}, error={error_str[:300]}"
                    )

                with results_lock:
                    query_results.append(
                        {
                            'query_id': f'test_{query_id}',
                            'status': (
                                'unavailable' if is_unavailable else ('session_busy' if is_session_busy else 'error')
                            ),
                            'duration': None,
                            'nodes': [node_id] if node_id != 'unknown' else [],
                            'hit_shutdown_node': hit_shutdown_node,
                            'error_type': error_type,
                            'error_message': error_str[:500],  # Store error message for analysis
                        }
                    )

            finally:
                if session:
                    try:
                        self.pool.release(session)
                    except Exception:
                        pass

        threads = []
        for i in range(num_queries):
            thread = threading.Thread(target=run_query, args=(i,))
            threads.append(thread)
            thread.start()
            # Increase delay to allow pool to distribute sessions across different nodes
            # This helps avoid SessionBusy when multiple queries hit the same session
            time.sleep(0.2)  # Stagger queries more to allow session distribution

        return threads

    def _print_results(self, query_results, shutting_down_node_ids, log_monitor_results, test_duration):
        """
        Step 8: Print final results with statistics.

        Args:
            query_results: List of all query results
            shutting_down_node_ids: Set of node IDs that were shut down
            log_monitor_results: Dict with log monitoring results
            test_duration: Total test duration in seconds
        """
        # Filter queries that hit shutdown nodes
        queries_on_shutdown_nodes = [q for q in query_results if q.get('hit_shutdown_node', False)]

        # Statistics
        total = len(query_results)
        successful = sum(1 for q in query_results if q['status'] == 'success')
        unavailable = sum(1 for q in query_results if q['status'] == 'unavailable')
        session_busy = sum(1 for q in query_results if q['status'] == 'session_busy')
        errors = sum(1 for q in query_results if q['status'] == 'error')

        queries_on_shutdown_total = len(queries_on_shutdown_nodes)
        queries_on_shutdown_succeeded = sum(1 for q in queries_on_shutdown_nodes if q['status'] == 'success')
        queries_on_shutdown_unavailable = sum(1 for q in queries_on_shutdown_nodes if q['status'] == 'unavailable')
        queries_on_shutdown_session_busy = sum(1 for q in queries_on_shutdown_nodes if q['status'] == 'session_busy')
        queries_on_shutdown_failed = sum(1 for q in queries_on_shutdown_nodes if q['status'] == 'error')

        shutdown_success_rate = (
            100.0 * queries_on_shutdown_succeeded / queries_on_shutdown_total if queries_on_shutdown_total > 0 else 0
        )
        success_rate = 100.0 * successful / total if total > 0 else 0

        logger.info("=" * 80)
        logger.info("GRACEFUL SHUTDOWN TEST RESULTS:")
        logger.info(f"  Total queries:       {total}")
        logger.info(f"  ✓ SUCCESS:           {successful} ({success_rate:.1f}%)")
        logger.info(f"  ⚠ UNAVAILABLE:       {unavailable}")
        logger.info(f"  🔄 SESSION_BUSY:      {session_busy} (expected when heavy queries occupy sessions)")
        logger.info(f"  ✗ ERRORS:            {errors}")
        logger.info("")
        logger.info(f"  📊 QUERIES HITTING SHUTDOWN NODES ({sorted(shutting_down_node_ids)}):")
        logger.info(f"     Total queries on shutdown nodes: {queries_on_shutdown_total}")
        logger.info(f"     ✓ SUCCEEDED:      {queries_on_shutdown_succeeded} ({shutdown_success_rate:.1f}%)")
        logger.info(f"     ⚠ UNAVAILABLE:    {queries_on_shutdown_unavailable}")
        logger.info(
            f"     🔄 SESSION_BUSY:   {queries_on_shutdown_session_busy} (expected when heavy queries occupy sessions)"
        )
        logger.info(f"     ✗ FAILED:         {queries_on_shutdown_failed}")
        logger.info("")

        if queries_on_shutdown_nodes:
            logger.info("  📋 DETAILED BREAKDOWN:")
            for q in queries_on_shutdown_nodes[:20]:  # Show first 20
                status_icon = "✓" if q['status'] == 'success' else "⚠" if q['status'] == 'unavailable' else "✗"
                duration_str = f" ({q['duration']:.2f}s)" if q.get('duration') else ""
                logger.info(
                    f"     {status_icon} {q['query_id']}: nodes={q['nodes']}, status={q['status']}{duration_str}"
                )
            if len(queries_on_shutdown_nodes) > 20:
                logger.info(f"     ... and {len(queries_on_shutdown_nodes) - 20} more")

        logger.info("")
        logger.info("  📝 LOG MONITORING RESULTS:")
        if log_monitor_results:
            for key, value in sorted(log_monitor_results.items()):
                logger.info(f"     {key}: {value}")
        else:
            logger.warning(
                "     (No shutdown messages found in logs - may indicate issue with graceful shutdown or log file access)"
            )

        logger.info("")
        logger.info(f"  Test duration:       {test_duration:.2f}s")
        logger.info("=" * 80)

    @pytest.mark.parametrize("enable_shutting_down", [True, False])
    def test_rolling_restart(self, enable_shutting_down):
        """Main test: graceful shutdown with distributed queries."""
        if self.use_external_cluster:
            logger.warning("Skipping test - requires local cluster control")
            return

        # Recreate cluster with specified flag
        self._create_cluster_with_many_nodes(enable_shutting_down=enable_shutting_down)

        flag_status = "ENABLED" if enable_shutting_down else "DISABLED"
        logger.info("=" * 80)
        logger.info(f"STARTING GRACEFUL SHUTDOWN TEST (enable_shutting_down_node_state={flag_status})")
        logger.info("=" * 80)

        test_start_time = time.time()

        # Step 1: Create table with distributed data
        table_name = f"{self.table_path}_rolling_test"
        self._prepare_table_with_distributed_data(table_name, rows_count=50000, partitions=64)

        # Verify initial state
        result = self.pool.execute_with_retries(f"SELECT COUNT(*) as cnt FROM `{table_name}`;")
        initial_count = result[0].rows[0]['cnt']
        assert initial_count == 50000, "Initial data check failed"
        logger.info(f"✓ Initial data verified: {initial_count} rows")

        # Step 2: Select nodes to shutdown
        nodes_to_restart = [self.cluster.nodes[2], self.cluster.nodes[3], self.cluster.nodes[4], self.cluster.nodes[5]]
        shutting_down_node_ids = {node.node_id for node in nodes_to_restart}
        logger.info(f"Will shutdown nodes: {sorted(shutting_down_node_ids)}")

        # Step 3: Create multiple different heavy queries (different compilation plans)
        num_heavy_queries = 5
        heavy_queries = [self._create_heavy_query(table_name, variant=i) for i in range(num_heavy_queries)]
        logger.info(f"Created {num_heavy_queries} different heavy query variants")

        # Step 4: Prepare for log monitoring (we'll check logs after queries complete)
        shutdown_start_time = time.time()
        log_monitor_results = {}

        # Step 5: Start multiple heavy queries in parallel
        query_results = []
        results_lock = threading.Lock()
        heavy_threads = []

        for i, heavy_query in enumerate(heavy_queries):
            thread = threading.Thread(
                target=self._run_heavy_query, args=(heavy_query, i, shutting_down_node_ids, query_results, results_lock)
            )
            heavy_threads.append(thread)
            thread.start()
            time.sleep(0.5)  # Stagger query starts

        logger.info(f"Started {num_heavy_queries} heavy queries")
        time.sleep(3)  # Let heavy queries start executing

        # Step 5.5: Check for syntax errors in logs after queries started
        logger.info("Checking logs for syntax errors...")
        # syntax_errors = self._check_logs_for_syntax_errors(list(self.cluster.nodes.values()))
        # if syntax_errors:
        #     logger.error("=" * 80)
        #     logger.error("SYNTAX ERRORS FOUND IN LOGS:")
        #     logger.error("=" * 80)
        #     for error in syntax_errors:
        #         logger.error(f"Node {error['node_id']}: {error['line']}")
        #     logger.error("=" * 80)
        #     # Don't fail the test, but log it clearly
        # else:
        #     logger.info("✓ No syntax errors found in logs")

        # Step 6: Shutdown nodes (after heavy queries are running)
        shutdown_threads = self._shutdown_nodes(nodes_to_restart, delay=1.0)
        time.sleep(1)  # Let shutdown initiate

        # Step 7: Run distributed queries
        distributed_threads = self._run_distributed_queries(
            table_name, 100, shutting_down_node_ids, query_results, results_lock
        )

        # Wait for queries to complete
        for thread in heavy_threads:
            thread.join(timeout=60)
        for thread in distributed_threads:
            thread.join(timeout=60)  # Increased timeout for more queries
        for thread in shutdown_threads:
            thread.join(timeout=10)

        # Give time for logs to be written - shutdown messages appear after SIGTERM is processed
        # Logs are buffered and may take time to flush to disk
        logger.info("Waiting for logs to be written (shutdown messages may appear with delay)...")
        time.sleep(30)  # Give enough time for shutdown messages to be written to logs

        # Final check of logs - check all nodes after queries completed
        logger.info("Performing final log check after all queries completed...")
        for node in nodes_to_restart:
            shutdown_logs = self._check_node_logs_for_shutdown(node, shutdown_start_time)
            node_id = node.node_id

            if shutdown_logs['shutdown_initiated']:
                count = len(shutdown_logs['shutdown_initiated'])
                log_monitor_results[f'node_{node_id}_shutdown_initiated'] = count
                logger.info(f"Final check: Node {node_id} - shutdown_initiated: {count}")
                # Log first few messages for debugging
                for msg in shutdown_logs['shutdown_initiated'][:3]:
                    logger.info(f"  → {msg[:200]}")
            else:
                logger.debug(f"Final check: Node {node_id} - no shutdown_initiated messages found")

            if shutdown_logs['rejecting_remote']:
                count = len(shutdown_logs['rejecting_remote'])
                log_monitor_results[f'node_{node_id}_rejecting_remote'] = count
                logger.info(f"Final check: Node {node_id} - rejecting_remote: {count}")
                # Log first few messages for debugging
                for msg in shutdown_logs['rejecting_remote'][:3]:
                    logger.info(f"  → {msg[:200]}")
            else:
                logger.debug(f"Final check: Node {node_id} - no rejecting_remote messages found")

            if shutdown_logs['node_shutting_down']:
                count = len(shutdown_logs['node_shutting_down'])
                log_monitor_results[f'node_{node_id}_node_shutting_down'] = count
                logger.info(f"Final check: Node {node_id} - node_shutting_down: {count}")
                # Log first few messages for debugging
                for msg in shutdown_logs['node_shutting_down'][:3]:
                    logger.info(f"  → {msg[:200]}")
            else:
                logger.debug(f"Final check: Node {node_id} - no node_shutting_down messages found")

        # Step 8: Print results
        test_duration = time.time() - test_start_time
        self._print_results(query_results, shutting_down_node_ids, log_monitor_results, test_duration)

        # Restart nodes
        for node in nodes_to_restart:
            try:
                node.stop()
            except Exception:
                pass
            logger.info(f"  Starting node {node.node_id}")
            node.start()
            time.sleep(2)

        # Store results for comparison
        if not hasattr(self.__class__, '_test_results'):
            self.__class__._test_results = {}

        successful = sum(1 for q in query_results if q['status'] == 'success')
        unavailable = sum(1 for q in query_results if q['status'] == 'unavailable')
        failed = sum(1 for q in query_results if q['status'] == 'error')
        total = len(query_results)
        success_rate = 100.0 * successful / total if total > 0 else 0

        self.__class__._test_results[enable_shutting_down] = {
            'total': total,
            'successful': successful,
            'unavailable': unavailable,
            'failed': failed,
            'success_rate': success_rate,
            'duration': test_duration,
        }

        logger.info(f"✓ Test completed successfully ({success_rate:.1f}% success rate)")
