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
        "decimal.handling.mode": "string",
        "time.precision.mode": "connect",
        "binary.handling.mode": "base64",
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
        f"jdbc:postgresql://{pg.host}:{pg.port}/{pg.database}"
        f"?sslmode=require&stringtype=unspecified"
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
        "auto.create": "false",
        "insert.mode": "upsert",
        "delete.enabled": "true",
        "pk.mode": "record_key",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "true",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "true",
        "transforms": "route",
        "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
        "transforms.route.regex": route_regex,
        "transforms.route.replacement": route_replacement,
        "errors.tolerance": "all",
        "errors.deadletterqueue.context.headers.enable": "true",
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
        base["errors.deadletterqueue.topic.name"] = f"dlq-{_sanitize_name(source.name)}-{_sanitize_name(db.name)}"

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


def build_all_connectors(
    config: AppConfig,
    kafka_ssl_host: str | None = None,
    kafka_ssl_port: int | None = None,
) -> list[ConnectorPlan]:
    """Build the full set of source + sink connector plans from config.

    If *kafka_ssl_host*/*kafka_ssl_port* are supplied they take precedence
    over the values in ``config.kafka``.  This allows callers that resolved
    the endpoint via the Aiven API to pass it in directly.
    """
    ssl_host = kafka_ssl_host or config.kafka.ssl_host
    ssl_port = kafka_ssl_port or config.kafka.ssl_port
    if not ssl_host or not ssl_port:
        raise ValueError(
            "Kafka SSL endpoint is not configured. Either set ssl_host/ssl_port "
            "in config.yaml or resolve via the Aiven API before calling build_all_connectors."
        )

    plans: list[ConnectorPlan] = []

    for source in config.mysql_sources:
        source_plan = build_source_connector(source, ssl_host, ssl_port)
        plans.append(source_plan)

        sink_plans = build_sink_connectors_for_source(
            source,
            config.postgresql_target,
            config.table_name_strategy,
        )
        plans.extend(sink_plans)

    return plans


def get_all_connector_names(
    config: AppConfig,
    kafka_ssl_host: str | None = None,
    kafka_ssl_port: int | None = None,
) -> list[str]:
    """Get all expected connector names from config."""
    return [plan.name for plan in build_all_connectors(config, kafka_ssl_host, kafka_ssl_port)]


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


# ── Discovery-Driven Connector Builders ──────────────────────────────

def build_discovered_connectors(
    cluster,
    selected_databases: list[str],
    database_mappings: list[dict],
    designated_keys: dict[str, list[str]],
    mysql_host: str,
    mysql_port: int,
    mysql_user: str,
    mysql_password: str,
    pg_host: str,
    pg_port: int,
    pg_user: str,
    pg_password: str,
    kafka_ssl_host: str,
    kafka_ssl_port: int,
    server_id: int = 1001,
    snapshot_mode: str = "initial",
) -> list[dict]:
    """Build connector configs from discovered schema data.

    Unlike build_all_connectors (config-driven), this uses actual discovery
    data to set per-table pk.fields and message.key.columns dynamically.
    """
    configs: list[dict] = []
    sanitized_host = _sanitize_name(mysql_host.split(".")[0])
    topic_prefix = f"mysql_cdc_{sanitized_host}"
    bootstrap_servers = f"{kafka_ssl_host}:{kafka_ssl_port}"

    # Collect all table names per database from the cluster discovery
    db_tables: dict[str, list] = {}
    for db in cluster.databases:
        if db.name not in selected_databases:
            continue
        db_tables[db.name] = db.tables

    # Source connector: one per MySQL cluster covering all selected databases
    db_list = ",".join(selected_databases)

    # Build message.key.columns for tables without PKs
    key_col_entries: list[str] = []
    for key_path, cols in designated_keys.items():
        if cols:
            key_col_entries.append(f"{key_path}:{','.join(cols)}")

    source_config: dict = {
        "name": f"debezium-source-{sanitized_host}",
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "database.hostname": mysql_host,
        "database.port": str(mysql_port),
        "database.user": mysql_user,
        "database.password": mysql_password,
        "database.server.id": str(server_id),
        "database.server.name": f"mysql_{sanitized_host}",
        "topic.prefix": topic_prefix,
        "database.include.list": db_list,
        "include.schema.changes": "true",
        "snapshot.mode": snapshot_mode,
        "decimal.handling.mode": "string",
        "time.precision.mode": "connect",
        "binary.handling.mode": "base64",
        "schema.history.internal.kafka.bootstrap.servers": bootstrap_servers,
        "schema.history.internal.kafka.topic": f"schema-history.{sanitized_host}",
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

    if key_col_entries:
        source_config["message.key.columns"] = ";".join(key_col_entries)

    configs.append(source_config)

    # Sink connectors: one per database mapping
    for mapping in database_mappings:
        src_db = mapping["source_db"]
        tgt_db = mapping["target_db"]

        tables = db_tables.get(src_db, [])
        if not tables:
            continue

        # Determine pk.fields from discovered PKs (use first table's PK as default)
        # For tables with different PKs, the JDBC sink uses record_key mode
        pk_fields_set: set[str] = set()
        for t in tables:
            if t.primary_key_columns:
                for col in t.primary_key_columns:
                    pk_fields_set.add(col)
            elif t.designated_key_columns:
                for col in t.designated_key_columns:
                    pk_fields_set.add(col)

        # Build topic list from discovered tables
        topics = ",".join(
            f"{topic_prefix}.{src_db}.{t.name}" for t in tables
        )

        connection_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{tgt_db}?sslmode=require&stringtype=unspecified"

        sink_config: dict = {
            "name": f"jdbc-sink-{_sanitize_name(src_db)}-to-{_sanitize_name(tgt_db)}",
            "connector.class": "io.aiven.connect.jdbc.JdbcSinkConnector",
            "connection.url": connection_url,
            "connection.user": pg_user,
            "connection.password": pg_password,
            "topics": topics,
            "auto.create": "false",
            "auto.evolve": "true",
            "insert.mode": "upsert",
            "delete.enabled": "true",
            "pk.mode": "record_key",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "true",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "true",
            "transforms": "route",
            "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.route.regex": "[^.]+\\.[^.]+\\.(.*)",
            "transforms.route.replacement": "$1",
            "errors.tolerance": "all",
            "errors.deadletterqueue.topic.name": f"dlq-{_sanitize_name(src_db)}",
            "errors.deadletterqueue.context.headers.enable": "true",
        }

        if pk_fields_set:
            sink_config["pk.fields"] = ",".join(sorted(pk_fields_set))

        configs.append(sink_config)

    return configs
