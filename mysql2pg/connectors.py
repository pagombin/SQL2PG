"""Connector configuration builders for Debezium source and JDBC sink."""

from __future__ import annotations

from dataclasses import dataclass

from .config import AppConfig, DatabaseSpec, MySQLSource, PostgreSQLTarget


def _sanitize_name(name: str) -> str:
    """Sanitize a name for use in connector/topic identifiers."""
    return name.replace(" ", "-").replace("_", "-").lower()


def _topic_prefix(source: MySQLSource) -> str:
    return f"mysql_cdc_{_sanitize_name(source.name)}"


def _source_connector_name(source: MySQLSource) -> str:
    return f"debezium-mysql-source-{_sanitize_name(source.name)}"


def _sink_connector_name(source: MySQLSource, db: DatabaseSpec, table: str | None = None) -> str:
    base = f"jdbc-pg-sink-{_sanitize_name(source.name)}-{_sanitize_name(db.name)}"
    if table:
        return f"{base}-{_sanitize_name(table)}"
    return base


@dataclass
class ConnectorPlan:
    """A planned connector deployment."""
    name: str
    config: dict
    connector_type: str  # "source" or "sink"
    source_name: str
    database_name: str
    table_name: str | None = None


def build_source_connector(
    source: MySQLSource,
    kafka_ssl_host: str,
    kafka_ssl_port: int,
) -> ConnectorPlan:
    """
    Build a Debezium MySQL source connector config for a single MySQL instance.
    One source connector handles all databases in the instance via database.include.list.
    """
    topic_prefix = _topic_prefix(source)
    db_list = ",".join(db.name for db in source.databases)
    bootstrap_servers = f"{kafka_ssl_host}:{kafka_ssl_port}"

    config = {
        "name": _source_connector_name(source),
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "database.hostname": source.host,
        "database.port": str(source.port),
        "database.user": source.username,
        "database.password": source.password,
        "database.server.id": str(source.server_id),
        "database.server.name": f"mysql_server_{_sanitize_name(source.name)}",
        "topic.prefix": topic_prefix,
        "database.include.list": db_list,
        "include.schema.changes": "true",
        "schema.history.internal.kafka.bootstrap.servers": bootstrap_servers,
        "schema.history.internal.kafka.topic": f"schema-history.{_sanitize_name(source.name)}",
        "schema.history.internal.consumer.security.protocol": "SSL",
        "schema.history.internal.producer.security.protocol": "SSL",
        "schema.history.internal.consumer.ssl.endpoint.identification.algorithm": "",
        "schema.history.internal.producer.ssl.endpoint.identification.algorithm": "",
        "schema.history.internal.consumer.ssl.keystore.location": "/run/aiven/keys/public.keystore.p12",
        "schema.history.internal.consumer.ssl.keystore.password": "password",
        "schema.history.internal.consumer.ssl.keystore.type": "PKCS12",
        "schema.history.internal.consumer.ssl.key.password": "password",
        "schema.history.internal.producer.ssl.keystore.location": "/run/aiven/keys/public.keystore.p12",
        "schema.history.internal.producer.ssl.keystore.password": "password",
        "schema.history.internal.producer.ssl.keystore.type": "PKCS12",
        "schema.history.internal.producer.ssl.key.password": "password",
        "schema.history.internal.consumer.ssl.truststore.location": "/run/aiven/keys/public.truststore.jks",
        "schema.history.internal.consumer.ssl.truststore.password": "password",
        "schema.history.internal.producer.ssl.truststore.location": "/run/aiven/keys/public.truststore.jks",
        "schema.history.internal.producer.ssl.truststore.password": "password",
        "transforms": "unwrap",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.drop.tombstones": "false",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "true",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "true",
    }

    return ConnectorPlan(
        name=_source_connector_name(source),
        config=config,
        connector_type="source",
        source_name=source.name,
        database_name=db_list,
    )


def _build_sink_base_config(
    pg: PostgreSQLTarget,
    table_name_strategy: str,
) -> dict:
    """Build the common JDBC sink connector configuration."""
    connection_url = (
        f"jdbc:postgresql://{pg.host}:{pg.port}/{pg.database}?sslmode=require"
    )

    route_replacement = "$1" if table_name_strategy == "as_is" else "$1_$2"
    route_regex = (
        "[^.]+\\.[^.]+\\.(.*)" if table_name_strategy == "as_is"
        else "[^.]+\\.([^.]+)\\.(.*)"
    )

    return {
        "connector.class": "io.aiven.connect.jdbc.JdbcSinkConnector",
        "connection.url": connection_url,
        "connection.user": pg.username,
        "connection.password": pg.password,
        "auto.create": "true",
        "insert.mode": "upsert",
        "delete.enabled": "true",
        "pk.mode": "record_key",
        "pk.fields": "id",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "true",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "true",
        "transforms": "unwrap,route",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.drop.tombstones": "false",
        "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.route.regex": route_regex,
        "transforms.route.replacement": route_replacement,
    }


def build_sink_connectors_for_source(
    source: MySQLSource,
    pg: PostgreSQLTarget,
    table_name_strategy: str,
) -> list[ConnectorPlan]:
    """
    Build JDBC sink connector configs for all databases/tables in a MySQL source.

    Strategy:
    - If tables are explicitly listed: one sink connector per database
      with all table topics listed explicitly.
    - If no tables listed: one sink connector per database using topics.regex
      to capture all tables.
    """
    topic_prefix = _topic_prefix(source)
    plans: list[ConnectorPlan] = []

    for db in source.databases:
        base = _build_sink_base_config(pg, table_name_strategy)
        name = _sink_connector_name(source, db)
        base["name"] = name

        if db.tables:
            topics = ",".join(
                f"{topic_prefix}.{db.name}.{table}" for table in db.tables
            )
            base["topics"] = topics
        else:
            regex = f"{topic_prefix}\\.{db.name}\\..*"
            base["topics.regex"] = regex

        plans.append(ConnectorPlan(
            name=name,
            config=base,
            connector_type="sink",
            source_name=source.name,
            database_name=db.name,
        ))

    return plans


def build_all_connectors(config: AppConfig) -> list[ConnectorPlan]:
    """Build the full set of source + sink connector plans from config."""
    plans: list[ConnectorPlan] = []

    for source in config.mysql_sources:
        source_plan = build_source_connector(
            source,
            config.kafka.ssl_host,
            config.kafka.ssl_port,
        )
        plans.append(source_plan)

        sink_plans = build_sink_connectors_for_source(
            source,
            config.postgresql_target,
            config.table_name_strategy,
        )
        plans.extend(sink_plans)

    return plans


def get_all_connector_names(config: AppConfig) -> list[str]:
    """Get all expected connector names from config."""
    return [plan.name for plan in build_all_connectors(config)]


def get_source_connector_names(config: AppConfig) -> list[str]:
    """Get all source connector names."""
    return [
        plan.name for plan in build_all_connectors(config)
        if plan.connector_type == "source"
    ]


def get_sink_connector_names(config: AppConfig) -> list[str]:
    """Get all sink connector names."""
    return [
        plan.name for plan in build_all_connectors(config)
        if plan.connector_type == "sink"
    ]
