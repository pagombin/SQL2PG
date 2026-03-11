"""Migration state machine orchestrator.

Coordinates the full migration lifecycle: discovery -> compatibility ->
schema creation -> connector deployment -> verification -> streaming.
State is persisted to disk so migrations survive server restarts.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from .aiven import AivenClient, AivenAPIError
from .compatibility import analyze_compatibility, report_to_dict
from .config import AppConfig
from .discovery import (
    cluster_to_summary,
    discover_mysql_cluster,
    discover_pg_cluster,
    pg_cluster_to_summary,
    rebuild_cluster_from_summary,
)
from .models import (
    DatabaseMapping,
    MigrationPhase,
    MigrationState,
)
from .schema import execute_ddl, execute_fk_constraints, generate_full_ddl
from .sizing import sizing_report_to_dict, validate_kafka_capacity
from .verification import live_comparison, verify_row_counts

MIGRATIONS_DIR = Path("/opt/mysql2pg/migrations")


def _ensure_dir():
    MIGRATIONS_DIR.mkdir(parents=True, exist_ok=True)


def _state_path(migration_id: str) -> Path:
    return MIGRATIONS_DIR / f"{migration_id}.json"


def save_state(state: MigrationState) -> None:
    _ensure_dir()
    state.updated_at = datetime.utcnow().isoformat() + "Z"
    with open(_state_path(state.migration_id), "w") as f:
        json.dump(state.to_dict(), f, indent=2, default=str)


def load_state(migration_id: str) -> MigrationState | None:
    path = _state_path(migration_id)
    if not path.exists():
        return None
    with open(path) as f:
        data = json.load(f)
    return MigrationState.from_dict(data)


def list_migrations() -> list[dict]:
    _ensure_dir()
    migrations: list[dict] = []
    for f in sorted(MIGRATIONS_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
        try:
            with open(f) as fh:
                data = json.load(fh)
            migrations.append({
                "migration_id": data.get("migration_id"),
                "phase": data.get("phase"),
                "created_at": data.get("created_at"),
                "updated_at": data.get("updated_at"),
                "mysql_host": data.get("mysql_host"),
                "selected_databases": data.get("selected_databases", []),
                "error_message": data.get("error_message", ""),
            })
        except Exception:
            continue
    return migrations


def delete_migration(migration_id: str) -> bool:
    path = _state_path(migration_id)
    if path.exists():
        path.unlink()
        return True
    return False


# ── Migration Creation ───────────────────────────────────────────────

def create_migration() -> MigrationState:
    state = MigrationState(
        migration_id=str(uuid.uuid4())[:8],
        phase=MigrationPhase.CREATED,
        created_at=datetime.utcnow().isoformat() + "Z",
        updated_at=datetime.utcnow().isoformat() + "Z",
    )
    state.add_event("info", "Migration created")
    save_state(state)
    return state


# ── Step Executors ───────────────────────────────────────────────────

def step_discover_source(
    state: MigrationState,
    host: str, port: int, username: str, password: str,
) -> MigrationState:
    """Connect to MySQL and discover all databases/schemas."""
    state.phase = MigrationPhase.DISCOVERING_SOURCE
    state.mysql_host = host
    state.mysql_port = port
    state.mysql_username = username
    state.mysql_password = password
    state.add_event("info", f"Discovering MySQL cluster at {host}:{port}")
    save_state(state)

    try:
        cluster = discover_mysql_cluster(host, port, username, password)
        state.source_cluster = cluster_to_summary(cluster)
        state.phase = MigrationPhase.SOURCE_DISCOVERED
        state.add_event("success", (
            f"Discovered {cluster.total_tables} tables across "
            f"{len(cluster.databases)} databases "
            f"({cluster.total_size / (1024**2):.1f} MB total)"
        ))
    except Exception as e:
        state.phase = MigrationPhase.FAILED
        state.error_message = str(e)
        state.add_event("error", f"Source discovery failed: {e}")

    save_state(state)
    return state


def step_select_databases(
    state: MigrationState,
    selected_databases: list[str],
    designated_keys: dict[str, list[str]] | None = None,
) -> MigrationState:
    """User selects which databases to migrate."""
    state.selected_databases = selected_databases
    if designated_keys:
        state.designated_keys = designated_keys
    state.phase = MigrationPhase.DATABASES_SELECTED
    state.add_event("info", f"Selected databases: {', '.join(selected_databases)}")
    save_state(state)
    return state


def step_discover_target(
    state: MigrationState,
    host: str, port: int, username: str, password: str,
    database: str = "defaultdb",
) -> MigrationState:
    """Connect to PostgreSQL and discover existing databases."""
    state.phase = MigrationPhase.DISCOVERING_TARGET
    state.pg_host = host
    state.pg_port = port
    state.pg_username = username
    state.pg_password = password
    state.pg_database = database
    state.add_event("info", f"Discovering PostgreSQL cluster at {host}:{port}")
    save_state(state)

    try:
        pg_cluster = discover_pg_cluster(host, port, username, password, database)
        state.target_cluster = pg_cluster_to_summary(pg_cluster)
        state.phase = MigrationPhase.TARGET_DISCOVERED
        state.add_event("success", (
            f"Discovered PostgreSQL {pg_cluster.version.split()[0] if pg_cluster.version else '?'} "
            f"with {len(pg_cluster.databases)} database(s). "
            f"PostGIS: {'available' if pg_cluster.has_postgis else 'not available'}. "
            f"Can create DB: {'yes' if pg_cluster.can_create_db else 'no'}."
        ))
    except Exception as e:
        state.phase = MigrationPhase.FAILED
        state.error_message = str(e)
        state.add_event("error", f"Target discovery failed: {e}")

    save_state(state)
    return state


def step_set_database_mappings(
    state: MigrationState,
    mappings: list[dict],
) -> MigrationState:
    """Set source->target database mappings."""
    state.database_mappings = mappings
    state.add_event("info", f"Set {len(mappings)} database mapping(s)")
    save_state(state)
    return state


def step_analyze_compatibility(state: MigrationState) -> MigrationState:
    """Run compatibility analysis on selected databases."""
    state.phase = MigrationPhase.ANALYZING_COMPATIBILITY
    state.add_event("info", "Running compatibility analysis")
    save_state(state)

    try:
        cluster = rebuild_cluster_from_summary(state.source_cluster)
        has_postgis = state.target_cluster.get("has_postgis", False)
        report = analyze_compatibility(cluster, state.selected_databases, has_postgis)
        state.compatibility_report = report_to_dict(report)

        if report.can_proceed:
            state.phase = MigrationPhase.COMPATIBILITY_REVIEWED
            state.add_event("success", (
                f"Compatibility analysis complete: "
                f"{len(report.blockers)} blockers, "
                f"{len(report.warnings)} warnings, "
                f"{len(report.info)} info items. Migration can proceed."
            ))
        else:
            state.phase = MigrationPhase.COMPATIBILITY_REVIEWED
            state.add_event("warn", (
                f"Compatibility analysis found {len(report.blockers)} BLOCKING issue(s). "
                f"These must be resolved before migration can proceed."
            ))
    except Exception as e:
        state.phase = MigrationPhase.FAILED
        state.error_message = str(e)
        state.add_event("error", f"Compatibility analysis failed: {e}")

    save_state(state)
    return state


def step_validate_kafka(
    state: MigrationState,
    app_config: AppConfig,
) -> MigrationState:
    """Validate Kafka cluster capacity for the migration."""
    state.phase = MigrationPhase.VALIDATING_KAFKA
    state.add_event("info", "Validating Kafka cluster capacity")
    save_state(state)

    try:
        client = AivenClient(app_config.aiven.token, app_config.aiven.project)
        cluster = rebuild_cluster_from_summary(state.source_cluster)
        report = validate_kafka_capacity(
            client, app_config.kafka.service_name,
            cluster, state.selected_databases,
        )
        state.sizing_report = sizing_report_to_dict(report)
        state.phase = MigrationPhase.KAFKA_VALIDATED
        status = "adequate" if report.adequate else "undersized"
        state.add_event(
            "success" if report.adequate else "warn",
            f"Kafka sizing: {status}. "
            f"{report.topics_needed} topics, {report.estimated_data_gb:.2f} GB, "
            f"~{report.estimated_duration_hours:.1f}h estimated."
        )
    except Exception as e:
        state.phase = MigrationPhase.FAILED
        state.error_message = str(e)
        state.add_event("error", f"Kafka validation failed: {e}")

    save_state(state)
    return state


def step_create_schema(state: MigrationState) -> MigrationState:
    """Generate and execute PostgreSQL DDL for the migration."""
    state.phase = MigrationPhase.CREATING_SCHEMA
    state.add_event("info", "Creating PostgreSQL schema")
    save_state(state)

    try:
        cluster = rebuild_cluster_from_summary(state.source_cluster)
        has_postgis = state.target_cluster.get("has_postgis", False)

        # Rebuild compatibility report
        report = analyze_compatibility(cluster, state.selected_databases, has_postgis)

        all_ddl: list[str] = []
        all_results: list[dict] = []

        # Use database mappings to determine target DB per source DB
        mappings = state.database_mappings
        if not mappings:
            mappings = [{"source_db": db, "target_db": db} for db in state.selected_databases]

        for mapping in mappings:
            src_db = mapping["source_db"]
            tgt_db = mapping["target_db"]
            db_obj = next((d for d in cluster.databases if d.name == src_db), None)
            if not db_obj:
                continue

            ddl = generate_full_ddl(
                db_obj, report,
                has_postgis=has_postgis,
                create_database=False,
                target_db_name=tgt_db,
            )
            all_ddl.extend(ddl)

            # Execute DDL (skip CREATE DATABASE and FK constraints)
            schema_ddl = [
                s for s in ddl
                if not s.strip().startswith("ALTER TABLE")
                and not s.strip().startswith("CREATE DATABASE")
                and not s.strip().startswith("-- Foreign key")
            ]

            results = execute_ddl(
                state.pg_host, state.pg_port,
                state.pg_username, state.pg_password,
                tgt_db, schema_ddl,
            )
            all_results.extend(results)
            state.add_event("info", f"Schema for {src_db} -> {tgt_db}: {len(results)} statements executed")

        state.ddl_statements = all_ddl
        errors = [r for r in all_results if r.get("status") == "error"]
        if errors:
            state.add_event("warn", f"{len(errors)} DDL error(s) encountered (may be pre-existing objects)")

        state.phase = MigrationPhase.SCHEMA_CREATED
        state.add_event("success", "Schema creation complete")
        state.progress = {
            "tables_total": sum(
                len([t for t in db.tables])
                for db in cluster.databases
                if db.name in state.selected_databases
            ),
            "tables_schema_created": sum(
                len([t for t in db.tables])
                for db in cluster.databases
                if db.name in state.selected_databases
            ),
        }

    except Exception as e:
        state.phase = MigrationPhase.FAILED
        state.error_message = str(e)
        state.add_event("error", f"Schema creation failed: {e}")

    save_state(state)
    return state


def step_deploy_connectors(
    state: MigrationState,
    app_config: AppConfig,
) -> MigrationState:
    """Deploy Debezium source and JDBC sink connectors."""
    state.phase = MigrationPhase.DEPLOYING_CONNECTORS
    state.add_event("info", "Deploying Kafka connectors")
    save_state(state)

    try:
        client = AivenClient(app_config.aiven.token, app_config.aiven.project)
        kafka_svc = app_config.kafka.service_name

        # Enable Kafka Connect + auto-create topics
        try:
            client.enable_kafka_connect(kafka_svc)
            state.add_event("info", "Kafka Connect enabled")
        except AivenAPIError:
            state.add_event("info", "Kafka Connect already enabled")

        try:
            client.enable_auto_create_topics(kafka_svc)
        except AivenAPIError:
            pass

        # Build connector configs from discovery data
        from .connectors import build_discovered_connectors

        cluster = rebuild_cluster_from_summary(state.source_cluster)
        mappings = state.database_mappings
        if not mappings:
            mappings = [{"source_db": db, "target_db": db} for db in state.selected_databases]

        connector_configs = build_discovered_connectors(
            cluster=cluster,
            selected_databases=state.selected_databases,
            database_mappings=mappings,
            designated_keys=state.designated_keys,
            mysql_host=state.mysql_host,
            mysql_port=state.mysql_port,
            mysql_user=state.mysql_username,
            mysql_password=state.mysql_password,
            pg_host=state.pg_host,
            pg_port=state.pg_port,
            pg_user=state.pg_username,
            pg_password=state.pg_password,
            kafka_ssl_host=app_config.kafka.ssl_host,
            kafka_ssl_port=app_config.kafka.ssl_port,
        )

        state.connector_configs = connector_configs
        deployed: list[str] = []
        failed: list[str] = []

        for cfg in connector_configs:
            name = cfg.get("name", "unknown")
            try:
                client.create_connector(kafka_svc, cfg)
                deployed.append(name)
                state.add_event("success", f"Deployed connector: {name}")
            except AivenAPIError as e:
                if e.status_code == 409:
                    deployed.append(name)
                    state.add_event("info", f"Connector already exists: {name}")
                else:
                    failed.append(name)
                    state.add_event("error", f"Failed to deploy {name}: {e}")

        if failed:
            state.phase = MigrationPhase.FAILED
            state.error_message = f"Failed to deploy connectors: {', '.join(failed)}"
        else:
            state.phase = MigrationPhase.CONNECTORS_RUNNING
            state.add_event("success", f"All {len(deployed)} connector(s) deployed")

    except Exception as e:
        state.phase = MigrationPhase.FAILED
        state.error_message = str(e)
        state.add_event("error", f"Connector deployment failed: {e}")

    save_state(state)
    return state


def step_apply_constraints(
    state: MigrationState,
) -> MigrationState:
    """Apply deferred FK constraints after initial data load."""
    state.phase = MigrationPhase.APPLYING_CONSTRAINTS
    state.add_event("info", "Applying foreign key constraints")
    save_state(state)

    try:
        cluster = rebuild_cluster_from_summary(state.source_cluster)
        mappings = state.database_mappings
        if not mappings:
            mappings = [{"source_db": db, "target_db": db} for db in state.selected_databases]

        total_ok = 0
        total_err = 0

        for mapping in mappings:
            src_db = mapping["source_db"]
            tgt_db = mapping["target_db"]
            db_obj = next((d for d in cluster.databases if d.name == src_db), None)
            if not db_obj:
                continue

            results = execute_fk_constraints(
                state.pg_host, state.pg_port,
                state.pg_username, state.pg_password,
                tgt_db, db_obj,
            )
            ok = sum(1 for r in results if r.get("status") == "ok")
            err = sum(1 for r in results if r.get("status") == "error")
            total_ok += ok
            total_err += err

            if err:
                for r in results:
                    if r.get("status") == "error":
                        state.add_event("warn", f"FK constraint error on {r.get('table')}: {r.get('error', '')[:100]}")

        state.phase = MigrationPhase.CONSTRAINTS_APPLIED
        state.add_event(
            "success" if total_err == 0 else "warn",
            f"FK constraints: {total_ok} applied, {total_err} errors",
        )

    except Exception as e:
        state.phase = MigrationPhase.FAILED
        state.error_message = str(e)
        state.add_event("error", f"Constraint application failed: {e}")

    save_state(state)
    return state


def step_verify(state: MigrationState) -> MigrationState:
    """Run post-migration verification."""
    state.phase = MigrationPhase.VERIFYING
    state.add_event("info", "Running post-migration verification")
    save_state(state)

    try:
        mappings = state.database_mappings
        if not mappings:
            mappings = [{"source_db": db, "target_db": db} for db in state.selected_databases]

        row_results = verify_row_counts(
            state.mysql_host, state.mysql_port,
            state.mysql_username, state.mysql_password,
            state.pg_host, state.pg_port,
            state.pg_username, state.pg_password,
            mappings,
        )

        synced = sum(1 for r in row_results if r.get("in_sync"))
        total = len([r for r in row_results if r.get("table") != "--"])

        state.progress["tables_verified"] = synced
        state.phase = MigrationPhase.VERIFIED
        state.add_event("success", f"Verification: {synced}/{total} tables in sync")

    except Exception as e:
        state.phase = MigrationPhase.FAILED
        state.error_message = str(e)
        state.add_event("error", f"Verification failed: {e}")

    save_state(state)
    return state


def step_start_streaming(state: MigrationState) -> MigrationState:
    """Transition to ongoing CDC streaming mode."""
    state.phase = MigrationPhase.STREAMING
    state.add_event("success", "Migration entered streaming (ongoing CDC) mode")
    save_state(state)
    return state


def step_complete(state: MigrationState) -> MigrationState:
    """Mark migration as completed."""
    state.phase = MigrationPhase.COMPLETED
    state.add_event("success", "Migration completed successfully")
    save_state(state)
    return state


# ── Live Status ──────────────────────────────────────────────────────

def get_live_comparison(state: MigrationState, include_samples: bool = False) -> dict:
    """Get live data comparison for an active migration."""
    mappings = state.database_mappings
    if not mappings:
        mappings = [{"source_db": db, "target_db": db} for db in state.selected_databases]

    cluster = None
    if include_samples and state.source_cluster:
        cluster = rebuild_cluster_from_summary(state.source_cluster)

    return live_comparison(
        state.mysql_host, state.mysql_port,
        state.mysql_username, state.mysql_password,
        state.pg_host, state.pg_port,
        state.pg_username, state.pg_password,
        mappings,
        cluster=cluster,
        include_samples=include_samples,
    )
