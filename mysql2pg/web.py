"""Flask web application wrapping MySQL2PG functionality over HTTPS."""

from __future__ import annotations

import json
import os
import ssl
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, jsonify, render_template, request

from . import __version__
from .aiven import AivenClient, AivenAPIError
from .certs import certs_exist, generate_self_signed_cert, get_cert_paths
from .config import AppConfig, load_config
from .connectors import (
    build_all_connectors,
    get_all_connector_names,
    ConnectorPlan,
)

TEMPLATE_DIR = Path(__file__).parent / "templates"

app = Flask(__name__, template_folder=str(TEMPLATE_DIR))
app.config["JSON_SORT_KEYS"] = False

_app_config: AppConfig | None = None
_config_path: str = "config.yaml"


def _get_config() -> AppConfig:
    global _app_config
    if _app_config is None:
        _app_config = load_config(_config_path)
    return _app_config


def _reload_config() -> AppConfig:
    global _app_config
    _app_config = load_config(_config_path)
    return _app_config


def _get_client() -> AivenClient:
    config = _get_config()
    return AivenClient(config.aiven.token, config.aiven.project)


def _get_kafka_service(config: AppConfig | None = None) -> str:
    """Get the Kafka service name from config or request args."""
    svc = request.args.get("service", "")
    if svc:
        return svc
    if config and config.kafka and config.kafka.service_name:
        return config.kafka.service_name
    return ""


def _resolve_ssl_for_service(
    service_name: str,
    config: AppConfig | None = None,
    client: AivenClient | None = None,
) -> tuple[str, int]:
    """Return Kafka SSL host/port, resolving from the Aiven API if needed."""
    if config and config.kafka and config.kafka.ssl_host and config.kafka.ssl_port:
        return config.kafka.ssl_host, config.kafka.ssl_port
    if client is None:
        client = _get_client()
    return client.get_kafka_ssl_endpoint(service_name)


def _plan_to_dict(plan: ConnectorPlan) -> dict:
    return {
        "name": plan.name,
        "connector_type": plan.connector_type,
        "source_name": plan.source_name,
        "database_name": plan.database_name,
        "table_name": plan.table_name,
    }


# ── Web UI Routes ─────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html", version=__version__)


# ── API: Health & Info ────────────────────────────────────────────────

@app.route("/api/health")
def health():
    return jsonify({"status": "ok", "version": __version__})


@app.route("/api/info")
def info():
    try:
        config = _get_config()
        result: dict = {
            "aiven_project": config.aiven.project,
        }
        if config.kafka and config.kafka.service_name:
            result["kafka_service"] = config.kafka.service_name
        if config.mysql_sources:
            result["mysql_sources"] = [
                {
                    "name": src.name, "host": src.host, "port": src.port,
                    "server_id": src.server_id,
                    "databases": [{"name": db.name, "tables": db.tables} for db in src.databases],
                }
                for src in config.mysql_sources
            ]
        if config.postgresql_target:
            result["postgresql_target"] = {
                "host": config.postgresql_target.host,
                "port": config.postgresql_target.port,
                "database": config.postgresql_target.database,
                "username": config.postgresql_target.username,
            }
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── API: Plan ─────────────────────────────────────────────────────────

@app.route("/api/plan")
def plan():
    try:
        config = _get_config()
        if not config.mysql_sources or not config.kafka:
            return jsonify({"error": "Plan requires mysql_sources and kafka in config.yaml. Use the Migration Wizard instead."}), 400
        ssl_host, ssl_port = _resolve_ssl_for_service(config.kafka.service_name, config)
        plans = build_all_connectors(config, ssl_host, ssl_port)
        return jsonify({
            "connectors": [_plan_to_dict(p) for p in plans],
            "source_count": sum(1 for p in plans if p.connector_type == "source"),
            "sink_count": sum(1 for p in plans if p.connector_type == "sink"),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── API: Status ───────────────────────────────────────────────────────

@app.route("/api/status")
def status():
    try:
        config = _get_config()
        client = _get_client()
        kafka_svc = _get_kafka_service(config)
        if not kafka_svc:
            return jsonify({"connectors": [], "message": "No Kafka service selected"})

        try:
            result = client.list_connectors(kafka_svc)
        except AivenAPIError as e:
            if e.status_code == 403:
                return jsonify({
                    "connectors": [],
                    "kafka_connect_disabled": True,
                    "message": f"Kafka Connect is not enabled on '{kafka_svc}'. Click Setup to enable it.",
                })
            raise

        connectors = result.get("connectors", [])

        statuses = []
        for conn_info in connectors:
            name = conn_info.get("name", "unknown")
            try:
                status_resp = client.get_connector_status(kafka_svc, name)
                state = status_resp.get("status", {}).get("state", "UNKNOWN")
                conn_type = status_resp.get("status", {}).get("type", "?")
                tasks = status_resp.get("status", {}).get("tasks", [])
            except AivenAPIError:
                state = "ERROR"
                conn_type = "?"
                tasks = []

            statuses.append({
                "name": name,
                "state": state,
                "type": conn_type,
                "tasks": [{"id": t.get("id"), "state": t.get("state", "?")} for t in tasks],
            })

        return jsonify({"connectors": statuses})
    except AivenAPIError as e:
        return jsonify({"error": str(e), "status_code": e.status_code}), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── API: Setup ────────────────────────────────────────────────────────

@app.route("/api/setup", methods=["POST"])
def setup():
    try:
        config = _get_config()
        client = _get_client()
        body = request.get_json(silent=True) or {}
        kafka_svc = body.get("service") or _get_kafka_service(config)
        if not kafka_svc:
            return jsonify({"error": "No Kafka service specified"}), 400

        steps = []

        client.enable_kafka_connect(kafka_svc)
        steps.append({"step": "enable_kafka_connect", "status": "ok"})

        client.enable_auto_create_topics(kafka_svc)
        steps.append({"step": "enable_auto_create_topics", "status": "ok"})

        return jsonify({"message": "Setup complete", "steps": steps})
    except AivenAPIError as e:
        return jsonify({"error": str(e), "status_code": e.status_code}), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── API: Deploy ───────────────────────────────────────────────────────

@app.route("/api/deploy", methods=["POST"])
def deploy():
    try:
        config = _get_config()
        if not config.mysql_sources or not config.kafka:
            return jsonify({"error": "Deploy requires mysql_sources and kafka in config.yaml. Use the Migration Wizard instead."}), 400
        client = _get_client()
        kafka_svc = config.kafka.service_name

        body = request.get_json(silent=True) or {}
        filter_type = body.get("type")

        ssl_host, ssl_port = _resolve_ssl_for_service(kafka_svc, config, client)
        all_plans = build_all_connectors(config, ssl_host, ssl_port)
        if filter_type == "source":
            plans = [p for p in all_plans if p.connector_type == "source"]
        elif filter_type == "sink":
            plans = [p for p in all_plans if p.connector_type == "sink"]
        else:
            plans = all_plans

        deployed = []
        failed = []

        for p in plans:
            try:
                client.create_connector(kafka_svc, p.config)
                deployed.append({"name": p.name, "status": "created"})
            except AivenAPIError as e:
                if e.status_code == 409:
                    deployed.append({"name": p.name, "status": "already_exists"})
                else:
                    failed.append({"name": p.name, "error": str(e)})

        return jsonify({
            "deployed": deployed,
            "failed": failed,
            "deployed_count": len(deployed),
            "failed_count": len(failed),
        })
    except AivenAPIError as e:
        return jsonify({"error": str(e), "status_code": e.status_code}), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── API: Teardown ─────────────────────────────────────────────────────

@app.route("/api/teardown", methods=["POST"])
def teardown():
    try:
        config = _get_config()
        client = _get_client()
        body = request.get_json(silent=True) or {}
        kafka_svc = body.get("service") or _get_kafka_service(config)
        if not kafka_svc:
            return jsonify({"error": "No Kafka service specified"}), 400

        delete_all = body.get("all", True)

        if delete_all:
            result = client.list_connectors(kafka_svc)
            names = [c.get("name") for c in result.get("connectors", []) if c.get("name")]
        elif config.kafka and config.mysql_sources:
            ssl_host, ssl_port = _resolve_ssl_for_service(kafka_svc, config, client)
            names = get_all_connector_names(config, ssl_host, ssl_port)
        else:
            result = client.list_connectors(kafka_svc)
            names = [c.get("name") for c in result.get("connectors", []) if c.get("name")]

        deleted = []
        failed = []

        for name in names:
            try:
                client.delete_connector(kafka_svc, name)
                deleted.append(name)
            except AivenAPIError as e:
                if e.status_code == 404:
                    deleted.append(name)
                else:
                    failed.append({"name": name, "error": str(e)})

        return jsonify({
            "deleted": deleted,
            "failed": failed,
            "deleted_count": len(deleted),
            "failed_count": len(failed),
        })
    except AivenAPIError as e:
        return jsonify({"error": str(e), "status_code": e.status_code}), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── API: Verify ───────────────────────────────────────────────────────

@app.route("/api/verify")
def verify():
    try:
        config = _get_config()
        client = _get_client()
        kafka_svc = _get_kafka_service(config)
        if not kafka_svc:
            return jsonify({"error": "No Kafka service specified. Select one first."}), 400

        features = client.get_service_features(kafka_svc)
        checks = {
            "kafka_connect": features.get("kafka_connect", False),
            "kafka_connect_service_integration": features.get(
                "kafka_connect_service_integration", False
            ),
        }

        try:
            service_data = client.get_service(kafka_svc)
            user_config = service_data.get("service", {}).get("user_config", {})
            kafka_config = user_config.get("kafka", {})
            auto_create = kafka_config.get("auto_create_topics_enable", False)
        except (AivenAPIError, KeyError):
            auto_create = None

        checks["auto_create_topics_enable"] = auto_create

        connect_uri = client.get_kafka_connect_uri(kafka_svc)

        return jsonify({
            "checks": checks,
            "kafka_connect_uri": connect_uri,
            "all_ok": all(v for v in checks.values() if v is not None),
        })
    except AivenAPIError as e:
        return jsonify({"error": str(e), "status_code": e.status_code}), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── API: Connector Actions ───────────────────────────────────────────

@app.route("/api/connector/<name>/pause", methods=["POST"])
def pause_connector(name: str):
    try:
        config = _get_config()
        client = _get_client()
        kafka_svc = _get_kafka_service(config)
        client.pause_connector(kafka_svc, name)
        return jsonify({"message": f"Connector '{name}' paused"})
    except AivenAPIError as e:
        return jsonify({"error": str(e)}), 502


@app.route("/api/connector/<name>/resume", methods=["POST"])
def resume_connector(name: str):
    try:
        config = _get_config()
        client = _get_client()
        kafka_svc = _get_kafka_service(config)
        client.resume_connector(kafka_svc, name)
        return jsonify({"message": f"Connector '{name}' resumed"})
    except AivenAPIError as e:
        return jsonify({"error": str(e)}), 502


@app.route("/api/connector/<name>/restart", methods=["POST"])
def restart_connector(name: str):
    try:
        config = _get_config()
        client = _get_client()
        kafka_svc = _get_kafka_service(config)
        client.restart_connector(kafka_svc, name)
        return jsonify({"message": f"Connector '{name}' restarted"})
    except AivenAPIError as e:
        return jsonify({"error": str(e)}), 502


@app.route("/api/restart-failed", methods=["POST"])
def restart_failed_tasks():
    """Restart all connectors that have FAILED tasks."""
    try:
        config = _get_config()
        client = _get_client()
        body = request.get_json(silent=True) or {}
        kafka_svc = body.get("service") or _get_kafka_service(config)
        if not kafka_svc:
            return jsonify({"error": "No Kafka service specified"}), 400

        result = client.list_connectors(kafka_svc)
        connectors = result.get("connectors", [])

        restarted = []
        failed = []
        for conn_info in connectors:
            name = conn_info.get("name", "")
            if not name:
                continue
            try:
                status_resp = client.get_connector_status(kafka_svc, name)
                tasks = status_resp.get("status", {}).get("tasks", [])
                has_failed = any(t.get("state") == "FAILED" for t in tasks)
                if has_failed:
                    client.restart_connector(kafka_svc, name)
                    restarted.append(name)
            except AivenAPIError as e:
                failed.append({"name": name, "error": str(e)})

        return jsonify({
            "restarted": restarted,
            "failed": failed,
            "restarted_count": len(restarted),
        })
    except AivenAPIError as e:
        return jsonify({"error": str(e), "status_code": e.status_code}), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── API: Config Reload ────────────────────────────────────────────────

@app.route("/api/config/reload", methods=["POST"])
def reload_config():
    try:
        _reload_config()
        return jsonify({"message": "Configuration reloaded"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── API: Kafka Services ──────────────────────────────────────────────

@app.route("/api/kafka-services")
def api_kafka_services():
    try:
        client = _get_client()
        services = client.list_kafka_services()
        return jsonify({"services": services})
    except AivenAPIError as e:
        return jsonify({"error": str(e), "status_code": e.status_code}), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ══════════════════════════════════════════════════════════════════════
# Migration Wizard API
# ══════════════════════════════════════════════════════════════════════

from .migration import (
    check_schema_drift,
    create_migration,
    delete_migration,
    get_live_comparison,
    list_migrations,
    load_state,
    save_state,
    step_analyze_compatibility,
    step_apply_constraints,
    step_complete,
    step_create_schema,
    step_deploy_connectors,
    step_discover_source,
    step_discover_target,
    step_select_databases,
    step_select_kafka_service,
    step_set_database_mappings,
    step_start_streaming,
    step_validate_kafka,
    step_verify,
)


@app.route("/migrate")
def migrate_wizard():
    return render_template("migrate.html", version=__version__)


# ── Migrations CRUD ──────────────────────────────────────────────────

@app.route("/api/migrations")
def api_list_migrations():
    return jsonify({"migrations": list_migrations()})


@app.route("/api/migrations", methods=["POST"])
def api_create_migration():
    state = create_migration()
    return jsonify({"migration_id": state.migration_id, "phase": state.phase.value})


@app.route("/api/migrations/<mid>")
def api_get_migration(mid: str):
    state = load_state(mid)
    if not state:
        return jsonify({"error": "Migration not found"}), 404
    return jsonify(state.to_dict())


@app.route("/api/migrations/<mid>", methods=["DELETE"])
def api_delete_migration(mid: str):
    if delete_migration(mid):
        return jsonify({"message": "Deleted"})
    return jsonify({"error": "Not found"}), 404


# ── Step: Select Kafka Service ───────────────────────────────────────

@app.route("/api/migrations/<mid>/select-kafka", methods=["POST"])
def api_select_kafka(mid: str):
    state = load_state(mid)
    if not state:
        return jsonify({"error": "Migration not found"}), 404

    body = request.get_json(silent=True) or {}
    service_name = body.get("service_name", "")
    if not service_name:
        return jsonify({"error": "Missing service_name"}), 400

    try:
        client = _get_client()
        client.get_service(service_name)
    except AivenAPIError as e:
        return jsonify({"error": f"Cannot connect to Kafka service '{service_name}': {e}"}), 400

    state = step_select_kafka_service(state, service_name)
    return jsonify(state.to_dict())


# ── Step: Discover Source ────────────────────────────────────────────

@app.route("/api/migrations/<mid>/discover-source", methods=["POST"])
def api_discover_source(mid: str):
    state = load_state(mid)
    if not state:
        return jsonify({"error": "Migration not found"}), 404

    body = request.get_json(silent=True) or {}
    for field in ("host", "port", "username", "password"):
        if field not in body:
            return jsonify({"error": f"Missing field: {field}"}), 400

    try:
        state = step_discover_source(
            state,
            host=body["host"],
            port=int(body["port"]),
            username=body["username"],
            password=body["password"],
        )
        return jsonify(state.to_dict())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Step: Select Databases ───────────────────────────────────────────

@app.route("/api/migrations/<mid>/select-databases", methods=["POST"])
def api_select_databases(mid: str):
    state = load_state(mid)
    if not state:
        return jsonify({"error": "Migration not found"}), 404

    body = request.get_json(silent=True) or {}
    databases = body.get("databases", [])
    if not databases:
        return jsonify({"error": "No databases selected"}), 400

    designated_keys = body.get("designated_keys", {})
    state = step_select_databases(state, databases, designated_keys)
    return jsonify(state.to_dict())


# ── Step: Discover Target ────────────────────────────────────────────

@app.route("/api/migrations/<mid>/discover-target", methods=["POST"])
def api_discover_target(mid: str):
    state = load_state(mid)
    if not state:
        return jsonify({"error": "Migration not found"}), 404

    body = request.get_json(silent=True) or {}
    for field in ("host", "port", "username", "password"):
        if field not in body:
            return jsonify({"error": f"Missing field: {field}"}), 400

    try:
        state = step_discover_target(
            state,
            host=body["host"],
            port=int(body["port"]),
            username=body["username"],
            password=body["password"],
            database=body.get("database", "defaultdb"),
        )
        return jsonify(state.to_dict())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Step: Set Database Mappings ──────────────────────────────────────

@app.route("/api/migrations/<mid>/database-mappings", methods=["POST"])
def api_set_mappings(mid: str):
    state = load_state(mid)
    if not state:
        return jsonify({"error": "Migration not found"}), 404

    body = request.get_json(silent=True) or {}
    mappings = body.get("mappings", [])
    if not mappings:
        return jsonify({"error": "No mappings provided"}), 400

    state = step_set_database_mappings(state, mappings)
    return jsonify(state.to_dict())


# ── Step: Analyze Compatibility ──────────────────────────────────────

@app.route("/api/migrations/<mid>/analyze", methods=["POST"])
def api_analyze_compatibility(mid: str):
    state = load_state(mid)
    if not state:
        return jsonify({"error": "Migration not found"}), 404

    try:
        state = step_analyze_compatibility(state)
        return jsonify(state.to_dict())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Step: Validate Kafka ─────────────────────────────────────────────

@app.route("/api/migrations/<mid>/validate-kafka", methods=["POST"])
def api_validate_kafka(mid: str):
    state = load_state(mid)
    if not state:
        return jsonify({"error": "Migration not found"}), 404

    try:
        config = _get_config()
        state = step_validate_kafka(state, config)
        return jsonify(state.to_dict())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Step: Create Schema ─────────────────────────────────────────────

@app.route("/api/migrations/<mid>/create-schema", methods=["POST"])
def api_create_schema(mid: str):
    state = load_state(mid)
    if not state:
        return jsonify({"error": "Migration not found"}), 404

    body = request.get_json(silent=True) or {}
    # Re-inject password since it's not persisted
    if body.get("pg_password"):
        state.pg_password = body["pg_password"]

    try:
        state = step_create_schema(state)
        return jsonify(state.to_dict())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Step: Deploy Connectors ─────────────────────────────────────────

@app.route("/api/migrations/<mid>/deploy-connectors", methods=["POST"])
def api_deploy_connectors(mid: str):
    state = load_state(mid)
    if not state:
        return jsonify({"error": "Migration not found"}), 404

    body = request.get_json(silent=True) or {}
    if body.get("mysql_password"):
        state.mysql_password = body["mysql_password"]
    if body.get("pg_password"):
        state.pg_password = body["pg_password"]

    try:
        config = _get_config()
        state = step_deploy_connectors(state, config)
        return jsonify(state.to_dict())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Step: Apply Constraints ─────────────────────────────────────────

@app.route("/api/migrations/<mid>/apply-constraints", methods=["POST"])
def api_apply_constraints(mid: str):
    state = load_state(mid)
    if not state:
        return jsonify({"error": "Migration not found"}), 404

    body = request.get_json(silent=True) or {}
    if body.get("pg_password"):
        state.pg_password = body["pg_password"]

    try:
        state = step_apply_constraints(state)
        return jsonify(state.to_dict())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Step: Verify ─────────────────────────────────────────────────────

@app.route("/api/migrations/<mid>/verify", methods=["POST"])
def api_verify_migration(mid: str):
    state = load_state(mid)
    if not state:
        return jsonify({"error": "Migration not found"}), 404

    body = request.get_json(silent=True) or {}
    if body.get("mysql_password"):
        state.mysql_password = body["mysql_password"]
    if body.get("pg_password"):
        state.pg_password = body["pg_password"]

    try:
        state = step_verify(state)
        return jsonify(state.to_dict())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Step: Streaming / Complete ───────────────────────────────────────

@app.route("/api/migrations/<mid>/start-streaming", methods=["POST"])
def api_start_streaming(mid: str):
    state = load_state(mid)
    if not state:
        return jsonify({"error": "Migration not found"}), 404
    state = step_start_streaming(state)
    return jsonify(state.to_dict())


@app.route("/api/migrations/<mid>/complete", methods=["POST"])
def api_complete_migration(mid: str):
    state = load_state(mid)
    if not state:
        return jsonify({"error": "Migration not found"}), 404
    state = step_complete(state)
    return jsonify(state.to_dict())


# ── Schema Drift Detection ───────────────────────────────────────────

@app.route("/api/migrations/<mid>/schema-drift", methods=["POST"])
def api_schema_drift(mid: str):
    state = load_state(mid)
    if not state:
        return jsonify({"error": "Migration not found"}), 404

    body = request.get_json(silent=True) or {}
    if body.get("mysql_password"):
        state.mysql_password = body["mysql_password"]
    if body.get("pg_password"):
        state.pg_password = body["pg_password"]

    if not state.mysql_password or not state.pg_password:
        return jsonify({"error": "Passwords required"}), 400

    try:
        result = check_schema_drift(state)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Live Data Comparison ─────────────────────────────────────────────

@app.route("/api/migrations/<mid>/compare")
def api_live_comparison(mid: str):
    state = load_state(mid)
    if not state:
        return jsonify({"error": "Migration not found"}), 404

    include_samples = request.args.get("samples", "false").lower() == "true"

    if not state.mysql_password or not state.pg_password:
        return jsonify({
            "error": "Passwords not available. Use POST with mysql_password and pg_password in the request body."
        }), 400

    try:
        result = get_live_comparison(state, include_samples=include_samples)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/migrations/<mid>/compare", methods=["POST"])
def api_live_comparison_post(mid: str):
    state = load_state(mid)
    if not state:
        return jsonify({"error": "Migration not found"}), 404

    body = request.get_json(silent=True) or {}
    if body.get("mysql_password"):
        state.mysql_password = body["mysql_password"]
    if body.get("pg_password"):
        state.pg_password = body["pg_password"]
    include_samples = body.get("include_samples", False)

    try:
        result = get_live_comparison(state, include_samples=include_samples)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Server Entry Point ───────────────────────────────────────────────

def create_app(config_path: str = "config.yaml") -> Flask:
    global _config_path
    _config_path = config_path
    return app


def run_server(
    config_path: str = "config.yaml",
    host: str = "0.0.0.0",
    port: int = 8443,
    cert_dir: str = "/opt/mysql2pg/certs",
    debug: bool = False,
    workers: int = 2,
) -> None:
    """Start the HTTPS server with self-signed certificates.

    Uses gunicorn for production serving.  Falls back to Flask's built-in
    server when gunicorn is unavailable (e.g. during local development).
    """
    global _config_path
    _config_path = config_path

    if not certs_exist(cert_dir):
        print(f"Generating self-signed certificates in {cert_dir}...")
        cert_path, key_path = generate_self_signed_cert(cert_dir)
        print(f"  Certificate: {cert_path}")
        print(f"  Private key: {key_path}")
    else:
        cert_path, key_path = get_cert_paths(cert_dir)
        print(f"Using existing certificates from {cert_dir}")

    print(f"\nMySQL2PG Web UI: https://{host}:{port}")
    print(f"API Health:      https://{host}:{port}/api/health")
    print(f"Config file:     {config_path}\n")

    if debug:
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ssl_context.load_cert_chain(str(cert_path), str(key_path))
        app.run(host=host, port=port, ssl_context=ssl_context, debug=True)
        return

    try:
        from gunicorn.app.base import BaseApplication

        class _GunicornApp(BaseApplication):
            def __init__(self, flask_app, options=None):
                self.flask_app = flask_app
                self.options = options or {}
                super().__init__()

            def load_config(self):
                for key, value in self.options.items():
                    if key in self.cfg.settings and value is not None:
                        self.cfg.set(key.lower(), value)

            def load(self):
                return self.flask_app

        options = {
            "bind": f"{host}:{port}",
            "workers": workers,
            "certfile": str(cert_path),
            "keyfile": str(key_path),
            "accesslog": "-",
            "errorlog": "-",
            "timeout": 120,
        }
        print(f"Starting gunicorn with {workers} worker(s)...")
        _GunicornApp(app, options).run()

    except ImportError:
        print("gunicorn not installed, falling back to Flask dev server")
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ssl_context.load_cert_chain(str(cert_path), str(key_path))
        app.run(host=host, port=port, ssl_context=ssl_context, debug=False)
