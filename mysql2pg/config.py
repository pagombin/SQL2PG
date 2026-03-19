"""Configuration loading, validation, and data models."""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class AivenConfig:
    token: str
    project: str


@dataclass
class KafkaConfig:
    service_name: str
    ssl_host: str = ""
    ssl_port: int = 0


@dataclass
class DatabaseSpec:
    name: str
    tables: list[str] = field(default_factory=list)


@dataclass
class MySQLSource:
    name: str
    host: str
    port: int
    username: str
    password: str
    server_id: int
    databases: list[DatabaseSpec] = field(default_factory=list)


@dataclass
class PostgreSQLTarget:
    host: str
    port: int
    username: str
    password: str
    database: str


@dataclass
class AppConfig:
    aiven: AivenConfig
    kafka: KafkaConfig
    mysql_sources: list[MySQLSource]
    postgresql_target: PostgreSQLTarget
    table_name_strategy: str = "as_is"


def _resolve_value(value: str) -> str:
    """Resolve a config value, substituting ${ENV_VAR} references."""
    if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
        env_var = value[2:-1]
        env_val = os.environ.get(env_var)
        if env_val is None:
            print(f"Warning: Environment variable {env_var} is not set", file=sys.stderr)
            return value
        return env_val
    return value


def _parse_database_spec(raw: dict | str) -> DatabaseSpec:
    if isinstance(raw, str):
        return DatabaseSpec(name=raw)
    return DatabaseSpec(
        name=raw["name"],
        tables=raw.get("tables", []),
    )


def _parse_mysql_source(raw: dict) -> MySQLSource:
    databases = [_parse_database_spec(db) for db in raw.get("databases", [])]
    return MySQLSource(
        name=raw["name"],
        host=_resolve_value(raw["host"]),
        port=int(raw.get("port", 25060)),
        username=_resolve_value(raw["username"]),
        password=_resolve_value(raw["password"]),
        server_id=int(raw.get("server_id", 1001)),
        databases=databases,
    )


def load_config(path: str | Path) -> AppConfig:
    """Load and validate configuration from a YAML file."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    with open(path) as f:
        raw = yaml.safe_load(f)

    _validate_raw_config(raw)

    aiven = AivenConfig(
        token=_resolve_value(raw["aiven"]["token"]),
        project=_resolve_value(raw["aiven"]["project"]),
    )

    kafka_raw = raw["kafka"]
    kafka = KafkaConfig(
        service_name=kafka_raw["service_name"],
        ssl_host=_resolve_value(kafka_raw["ssl_host"]) if "ssl_host" in kafka_raw else "",
        ssl_port=int(kafka_raw["ssl_port"]) if "ssl_port" in kafka_raw else 0,
    )

    mysql_sources = [_parse_mysql_source(src) for src in raw["mysql_sources"]]
    _validate_unique_server_ids(mysql_sources)
    _validate_unique_source_names(mysql_sources)

    pg = raw["postgresql_target"]
    postgresql_target = PostgreSQLTarget(
        host=_resolve_value(pg["host"]),
        port=int(pg.get("port", 25060)),
        username=_resolve_value(pg["username"]),
        password=_resolve_value(pg["password"]),
        database=pg.get("database", "defaultdb"),
    )

    strategy = raw.get("table_name_strategy", "as_is")
    if strategy not in ("as_is", "prefixed"):
        raise ValueError(f"Invalid table_name_strategy: {strategy}. Must be 'as_is' or 'prefixed'.")

    return AppConfig(
        aiven=aiven,
        kafka=kafka,
        mysql_sources=mysql_sources,
        postgresql_target=postgresql_target,
        table_name_strategy=strategy,
    )


def _validate_raw_config(raw: dict) -> None:
    """Validate top-level structure of raw config."""
    required_sections = ["aiven", "kafka", "mysql_sources", "postgresql_target"]
    for section in required_sections:
        if section not in raw:
            raise ValueError(f"Missing required config section: '{section}'")

    if not raw.get("mysql_sources"):
        raise ValueError("At least one MySQL source must be configured")

    for key in ["token", "project"]:
        if key not in raw["aiven"]:
            raise ValueError(f"Missing required aiven config: '{key}'")

    if "service_name" not in raw["kafka"]:
        raise ValueError("Missing required kafka config: 'service_name'")

    for key in ["host", "username", "password"]:
        if key not in raw["postgresql_target"]:
            raise ValueError(f"Missing required postgresql_target config: '{key}'")


def _validate_unique_server_ids(sources: list[MySQLSource]) -> None:
    seen: dict[int, str] = {}
    for src in sources:
        if src.server_id in seen:
            raise ValueError(
                f"Duplicate server_id {src.server_id} in sources "
                f"'{seen[src.server_id]}' and '{src.name}'. Each source must have a unique server_id."
            )
        seen[src.server_id] = src.name


def _validate_unique_source_names(sources: list[MySQLSource]) -> None:
    seen: set[str] = set()
    for src in sources:
        if src.name in seen:
            raise ValueError(f"Duplicate source name: '{src.name}'")
        seen.add(src.name)
