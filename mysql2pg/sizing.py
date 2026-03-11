"""Kafka cluster capacity validation for migration workloads."""

from __future__ import annotations

import math
from typing import Any

from .aiven import AivenClient, AivenAPIError
from .models import DiscoveredCluster, KafkaSizingReport


# Rough per-node throughput estimates (MB/s sustained writes) by vCPU count.
# These are conservative estimates for DigitalOcean Managed Kafka.
_THROUGHPUT_PER_NODE: dict[int, float] = {
    2: 5.0,
    3: 8.0,
    4: 12.0,
    6: 20.0,
    8: 30.0,
    12: 45.0,
    16: 60.0,
}

# Connector tasks per vCPU (heuristic: 2 tasks per core)
_TASKS_PER_CPU = 2


def _estimate_throughput(cpu_per_node: int) -> float:
    if cpu_per_node in _THROUGHPUT_PER_NODE:
        return _THROUGHPUT_PER_NODE[cpu_per_node]
    closest = min(_THROUGHPUT_PER_NODE.keys(), key=lambda k: abs(k - cpu_per_node))
    return _THROUGHPUT_PER_NODE[closest]


def _parse_plan_resources(service_data: dict) -> dict:
    """Extract node count, CPU, RAM, storage from Aiven service response."""
    service = service_data.get("service", {})
    plan = service.get("plan", "unknown")
    node_count = service.get("node_count", 3)
    node_states = service.get("node_states", [])
    if node_states:
        node_count = max(node_count, len(node_states))

    # Aiven service response includes various resource fields
    components = service.get("components", [])
    cpu_per_node = 0
    storage_gb = 0.0

    # Try to get from service_type_description or plan metadata
    service_uri = service.get("service_uri", "")

    # Parse from plan name patterns like "business-4" or "hobbyist"
    plan_lower = plan.lower()
    if "hobbyist" in plan_lower or "startup" in plan_lower:
        cpu_per_node = 2
        storage_gb = 30.0
    elif "business-4" in plan_lower:
        cpu_per_node = 4
        storage_gb = 100.0
    elif "business-8" in plan_lower:
        cpu_per_node = 8
        storage_gb = 200.0
    elif "premium" in plan_lower:
        cpu_per_node = 8
        storage_gb = 500.0
    else:
        # Fallback: try to infer from disk_space_mb
        disk_mb = service.get("disk_space_mb", 0)
        if disk_mb:
            storage_gb = disk_mb / 1024.0
        # Fallback CPU estimate from node count
        cpu_per_node = max(2, node_count)

    # Override with actual disk_space_mb if available
    disk_mb = service.get("disk_space_mb", 0)
    if disk_mb > 0:
        storage_gb = disk_mb / 1024.0

    return {
        "plan": plan,
        "node_count": node_count,
        "cpu_per_node": cpu_per_node,
        "storage_gb": storage_gb,
    }


def validate_kafka_capacity(
    client: AivenClient,
    kafka_service_name: str,
    cluster: DiscoveredCluster,
    selected_databases: list[str],
    replication_factor: int = 2,
    retention_hours: int = 72,
    desired_migration_hours: float = 4.0,
) -> KafkaSizingReport:
    """Validate that the Kafka cluster can handle the migration workload."""

    # Count tables and data across selected databases
    total_tables = 0
    total_data_bytes = 0
    total_rows = 0
    for db in cluster.databases:
        if db.name not in selected_databases:
            continue
        total_tables += db.table_count
        total_data_bytes += db.total_size
        total_rows += db.total_rows

    total_data_gb = total_data_bytes / (1024 ** 3)

    # Topics: 1 per table + 1 schema-history per source connector + internal topics
    topics_needed = total_tables + 2  # +1 schema-history, +1 config topic

    # Partitions: 1 per topic for ordering guarantee during migration
    partitions_needed = topics_needed

    # Connectors: 1 source + 1 sink per selected database
    num_selected_dbs = len(selected_databases)
    connectors_needed = 1 + num_selected_dbs  # 1 source connector + N sink connectors

    # Get cluster resources
    try:
        service_data = client.get_service(kafka_service_name)
        resources = _parse_plan_resources(service_data)
    except AivenAPIError:
        resources = {
            "plan": "unknown",
            "node_count": 3,
            "cpu_per_node": 3,
            "storage_gb": 50.0,
        }

    node_count = resources["node_count"]
    cpu_per_node = resources["cpu_per_node"]
    storage_gb = resources["storage_gb"]
    plan_name = resources["plan"]

    # Throughput calculation
    throughput_per_node = _estimate_throughput(cpu_per_node)
    total_throughput_mbps = throughput_per_node * node_count

    # Required throughput for desired migration window
    if total_data_gb > 0 and desired_migration_hours > 0:
        required_mbps = (total_data_gb * 1024) / (desired_migration_hours * 3600)
    else:
        required_mbps = 0

    # Estimated duration at cluster capacity
    if total_throughput_mbps > 0 and total_data_gb > 0:
        estimated_hours = (total_data_gb * 1024) / (total_throughput_mbps * 3600)
    else:
        estimated_hours = 0

    # Max connector tasks
    max_tasks = cpu_per_node * _TASKS_PER_CPU * node_count

    # Storage requirement: data * replication_factor (for Kafka storage during migration)
    # Plus retention buffer
    storage_needed_gb = total_data_gb * replication_factor * 1.5  # 1.5x safety margin

    # Build recommendations
    recommendations: list[str] = []
    adequate = True

    if topics_needed > 500:
        recommendations.append(
            f"High topic count ({topics_needed}). Performance may degrade above 500 topics. "
            f"Consider batching migrations or upgrading the cluster."
        )
        if topics_needed > 1000:
            adequate = False

    if partitions_needed > 1000 * node_count:
        recommendations.append(
            f"Partition count ({partitions_needed}) exceeds recommended limit "
            f"of {1000 * node_count} for a {node_count}-node cluster."
        )
        adequate = False

    if required_mbps > total_throughput_mbps * 0.8:
        recommendations.append(
            f"Required throughput ({required_mbps:.1f} MB/s) approaches cluster capacity "
            f"({total_throughput_mbps:.1f} MB/s). Consider a longer migration window or upgrading."
        )
        if required_mbps > total_throughput_mbps:
            adequate = False

    if storage_needed_gb > storage_gb * 0.8:
        recommendations.append(
            f"Estimated Kafka storage needed ({storage_needed_gb:.1f} GB) approaches "
            f"available storage ({storage_gb:.1f} GB). Consider adding storage or reducing retention."
        )
        if storage_needed_gb > storage_gb:
            adequate = False

    if connectors_needed > max_tasks:
        recommendations.append(
            f"Connector count ({connectors_needed}) exceeds estimated task capacity ({max_tasks}). "
            f"Upgrade cluster or reduce parallel connectors."
        )
        adequate = False

    if not recommendations:
        recommendations.append(
            f"Cluster is adequately sized for this migration "
            f"({total_tables} tables, {total_data_gb:.2f} GB). "
            f"Estimated migration time: {estimated_hours:.1f} hours."
        )

    return KafkaSizingReport(
        adequate=adequate,
        topics_needed=topics_needed,
        partitions_needed=partitions_needed,
        estimated_data_gb=round(total_data_gb, 3),
        estimated_duration_hours=round(estimated_hours, 2),
        cluster_plan=plan_name,
        cluster_nodes=node_count,
        cluster_cpu_per_node=cpu_per_node,
        cluster_storage_gb=round(storage_gb, 2),
        max_connector_tasks=max_tasks,
        connectors_needed=connectors_needed,
        recommendations=recommendations,
    )


def sizing_report_to_dict(report: KafkaSizingReport) -> dict:
    """Serialize a KafkaSizingReport to a JSON-safe dict."""
    return {
        "adequate": report.adequate,
        "topics_needed": report.topics_needed,
        "partitions_needed": report.partitions_needed,
        "estimated_data_gb": report.estimated_data_gb,
        "estimated_duration_hours": report.estimated_duration_hours,
        "cluster_plan": report.cluster_plan,
        "cluster_nodes": report.cluster_nodes,
        "cluster_cpu_per_node": report.cluster_cpu_per_node,
        "cluster_storage_gb": report.cluster_storage_gb,
        "max_connector_tasks": report.max_connector_tasks,
        "connectors_needed": report.connectors_needed,
        "recommendations": report.recommendations,
    }
