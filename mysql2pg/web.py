"""Flask web application wrapping MySQL2PG functionality over HTTPS."""

from __future__ import annotations

import json
import os
import ssl
import threading
import traceback
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
_background_tasks: dict[str, dict] = {}


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
        sources = []
        for src in config.mysql_sources:
            dbs = [{"name": db.name, "tables": db.tables} for db in src.databases]
            sources.append({
                "name": src.name,
                "host": src.host,
                "port": src.port,
                "server_id": src.server_id,
                "databases": dbs,
            })
        return jsonify({
            "aiven_project": config.aiven.project,
            "kafka_service": config.kafka.service_name,
            "kafka_ssl": f"{config.kafka.ssl_host}:{config.kafka.ssl_port}",
            "table_name_strategy": config.table_name_strategy,
            "mysql_sources": sources,
            "postgresql_target": {
                "host": config.postgresql_target.host,
                "port": config.postgresql_target.port,
                "database": config.postgresql_target.database,
                "username": config.postgresql_target.username,
            },
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── API: Plan ─────────────────────────────────────────────────────────

@app.route("/api/plan")
def plan():
    try:
        config = _get_config()
        plans = build_all_connectors(config)
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
        kafka_svc = config.kafka.service_name

        result = client.list_connectors(kafka_svc)
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
        kafka_svc = config.kafka.service_name

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
        client = _get_client()
        kafka_svc = config.kafka.service_name

        body = request.get_json(silent=True) or {}
        filter_type = body.get("type")

        all_plans = build_all_connectors(config)
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
        kafka_svc = config.kafka.service_name

        body = request.get_json(silent=True) or {}
        delete_all = body.get("all", False)

        if delete_all:
            result = client.list_connectors(kafka_svc)
            names = [c.get("name") for c in result.get("connectors", []) if c.get("name")]
        else:
            names = get_all_connector_names(config)

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
        kafka_svc = config.kafka.service_name

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
        client.pause_connector(config.kafka.service_name, name)
        return jsonify({"message": f"Connector '{name}' paused"})
    except AivenAPIError as e:
        return jsonify({"error": str(e)}), 502


@app.route("/api/connector/<name>/resume", methods=["POST"])
def resume_connector(name: str):
    try:
        config = _get_config()
        client = _get_client()
        client.resume_connector(config.kafka.service_name, name)
        return jsonify({"message": f"Connector '{name}' resumed"})
    except AivenAPIError as e:
        return jsonify({"error": str(e)}), 502


@app.route("/api/connector/<name>/restart", methods=["POST"])
def restart_connector(name: str):
    try:
        config = _get_config()
        client = _get_client()
        client.restart_connector(config.kafka.service_name, name)
        return jsonify({"message": f"Connector '{name}' restarted"})
    except AivenAPIError as e:
        return jsonify({"error": str(e)}), 502


# ── API: Config Reload ────────────────────────────────────────────────

@app.route("/api/config/reload", methods=["POST"])
def reload_config():
    try:
        _reload_config()
        return jsonify({"message": "Configuration reloaded"})
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
) -> None:
    """Start the HTTPS server with self-signed certificates."""
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

    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_context.load_cert_chain(str(cert_path), str(key_path))

    print(f"\nMySQL2PG Web UI: https://{host}:{port}")
    print(f"API Health:      https://{host}:{port}/api/health")
    print(f"Config file:     {config_path}\n")

    app.run(
        host=host,
        port=port,
        ssl_context=ssl_context,
        debug=debug,
    )
