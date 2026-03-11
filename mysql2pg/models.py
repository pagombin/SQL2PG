"""Data models for schema representation, discovery results, and migration state."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any


# ── Enums ─────────────────────────────────────────────────────────────

class Severity(str, enum.Enum):
    BLOCK = "block"
    WARN = "warn"
    INFO = "info"


class MigrationPhase(str, enum.Enum):
    CREATED = "created"
    DISCOVERING_SOURCE = "discovering_source"
    SOURCE_DISCOVERED = "source_discovered"
    SELECTING_DATABASES = "selecting_databases"
    DATABASES_SELECTED = "databases_selected"
    DISCOVERING_TARGET = "discovering_target"
    TARGET_DISCOVERED = "target_discovered"
    ANALYZING_COMPATIBILITY = "analyzing_compatibility"
    COMPATIBILITY_REVIEWED = "compatibility_reviewed"
    VALIDATING_KAFKA = "validating_kafka"
    KAFKA_VALIDATED = "kafka_validated"
    CREATING_SCHEMA = "creating_schema"
    SCHEMA_CREATED = "schema_created"
    DEPLOYING_CONNECTORS = "deploying_connectors"
    CONNECTORS_RUNNING = "connectors_running"
    SNAPSHOT_IN_PROGRESS = "snapshot_in_progress"
    SNAPSHOT_COMPLETE = "snapshot_complete"
    APPLYING_CONSTRAINTS = "applying_constraints"
    CONSTRAINTS_APPLIED = "constraints_applied"
    VERIFYING = "verifying"
    VERIFIED = "verified"
    STREAMING = "streaming"
    COMPLETED = "completed"
    FAILED = "failed"


# ── Column / Table / Database Models ─────────────────────────────────

@dataclass
class DiscoveredColumn:
    name: str
    ordinal_position: int
    data_type: str
    column_type: str  # Full type string from MySQL, e.g. "int unsigned", "enum('a','b')"
    is_nullable: bool
    column_default: str | None
    is_auto_increment: bool
    is_unsigned: bool
    is_zerofill: bool
    character_maximum_length: int | None = None
    numeric_precision: int | None = None
    numeric_scale: int | None = None
    datetime_precision: int | None = None
    charset: str | None = None
    collation: str | None = None
    enum_values: list[str] = field(default_factory=list)
    set_values: list[str] = field(default_factory=list)
    on_update: str | None = None  # e.g. "CURRENT_TIMESTAMP"
    generation_expression: str | None = None
    is_virtual: bool = False
    is_stored: bool = False
    comment: str = ""


@dataclass
class IndexColumn:
    column_name: str
    seq_in_index: int
    sub_part: int | None = None  # prefix length
    collation: str | None = None  # A=asc, D=desc


@dataclass
class DiscoveredIndex:
    name: str
    columns: list[IndexColumn]
    is_unique: bool
    index_type: str  # BTREE, HASH, FULLTEXT, SPATIAL
    is_primary: bool = False
    comment: str = ""


@dataclass
class ForeignKey:
    constraint_name: str
    table_schema: str
    table_name: str
    columns: list[str]
    referenced_schema: str
    referenced_table: str
    referenced_columns: list[str]
    on_update: str  # CASCADE, SET NULL, RESTRICT, NO ACTION, SET DEFAULT
    on_delete: str


@dataclass
class DiscoveredTrigger:
    name: str
    event: str  # INSERT, UPDATE, DELETE
    timing: str  # BEFORE, AFTER
    statement: str
    table_name: str


@dataclass
class DiscoveredTable:
    schema_name: str
    name: str
    engine: str
    row_count: int
    data_length: int  # bytes
    index_length: int  # bytes
    auto_increment_value: int | None
    table_collation: str | None
    comment: str
    columns: list[DiscoveredColumn] = field(default_factory=list)
    indexes: list[DiscoveredIndex] = field(default_factory=list)
    foreign_keys: list[ForeignKey] = field(default_factory=list)
    referenced_by: list[ForeignKey] = field(default_factory=list)
    triggers: list[DiscoveredTrigger] = field(default_factory=list)
    primary_key_columns: list[str] = field(default_factory=list)
    has_primary_key: bool = False
    designated_key_columns: list[str] = field(default_factory=list)

    @property
    def total_size(self) -> int:
        return self.data_length + self.index_length

    @property
    def effective_key_columns(self) -> list[str]:
        if self.primary_key_columns:
            return self.primary_key_columns
        return self.designated_key_columns


@dataclass
class DiscoveredView:
    schema_name: str
    name: str
    definition: str
    is_updatable: bool


@dataclass
class DiscoveredRoutine:
    schema_name: str
    name: str
    routine_type: str  # PROCEDURE or FUNCTION
    definition: str | None
    data_type: str | None  # return type for functions


@dataclass
class DiscoveredDatabase:
    name: str
    charset: str
    collation: str
    tables: list[DiscoveredTable] = field(default_factory=list)
    views: list[DiscoveredView] = field(default_factory=list)
    routines: list[DiscoveredRoutine] = field(default_factory=list)

    @property
    def table_count(self) -> int:
        return len(self.tables)

    @property
    def total_rows(self) -> int:
        return sum(t.row_count for t in self.tables)

    @property
    def total_size(self) -> int:
        return sum(t.total_size for t in self.tables)

    @property
    def tables_without_pk(self) -> list[DiscoveredTable]:
        return [t for t in self.tables if not t.has_primary_key]


@dataclass
class DiscoveredCluster:
    host: str
    port: int
    version: str
    databases: list[DiscoveredDatabase] = field(default_factory=list)
    sql_mode: str = ""
    global_charset: str = ""

    @property
    def total_tables(self) -> int:
        return sum(db.table_count for db in self.databases)

    @property
    def total_size(self) -> int:
        return sum(db.total_size for db in self.databases)


# ── PostgreSQL Target Models ─────────────────────────────────────────

@dataclass
class PgDatabase:
    name: str
    owner: str
    encoding: str
    collation: str
    size_bytes: int


@dataclass
class PgColumn:
    name: str
    data_type: str
    is_nullable: bool
    column_default: str | None
    ordinal_position: int


@dataclass
class PgTable:
    schema_name: str
    name: str
    columns: list[PgColumn] = field(default_factory=list)
    row_count: int = 0


@dataclass
class PgCluster:
    host: str
    port: int
    version: str
    databases: list[PgDatabase] = field(default_factory=list)
    available_extensions: list[str] = field(default_factory=list)
    has_postgis: bool = False
    can_create_db: bool = False


# ── Compatibility Models ─────────────────────────────────────────────

@dataclass
class TypeMapping:
    mysql_type: str
    mysql_column_type: str
    pg_type: str
    severity: Severity
    note: str
    requires_enum_type: bool = False
    enum_type_name: str = ""


@dataclass
class CompatibilityIssue:
    severity: Severity
    database: str
    table: str
    column: str
    mysql_type: str
    pg_type: str
    message: str


@dataclass
class EnumTypeDef:
    type_name: str
    values: list[str]
    database: str


@dataclass
class TriggerDef:
    function_name: str
    trigger_name: str
    table_name: str
    column_name: str
    database: str


@dataclass
class CompatibilityReport:
    blockers: list[CompatibilityIssue] = field(default_factory=list)
    warnings: list[CompatibilityIssue] = field(default_factory=list)
    info: list[CompatibilityIssue] = field(default_factory=list)
    column_mappings: dict[str, TypeMapping] = field(default_factory=dict)
    enum_types_needed: list[EnumTypeDef] = field(default_factory=list)
    triggers_needed: list[TriggerDef] = field(default_factory=list)
    tables_without_pk: list[str] = field(default_factory=list)

    @property
    def can_proceed(self) -> bool:
        return len(self.blockers) == 0

    @property
    def total_issues(self) -> int:
        return len(self.blockers) + len(self.warnings) + len(self.info)


# ── Kafka Sizing Models ──────────────────────────────────────────────

@dataclass
class KafkaSizingReport:
    adequate: bool
    topics_needed: int
    partitions_needed: int
    estimated_data_gb: float
    estimated_duration_hours: float
    cluster_plan: str
    cluster_nodes: int
    cluster_cpu_per_node: int
    cluster_storage_gb: float
    max_connector_tasks: int
    connectors_needed: int
    recommendations: list[str] = field(default_factory=list)


# ── Database Mapping ─────────────────────────────────────────────────

@dataclass
class DatabaseMapping:
    source_db: str
    target_db: str
    create_target: bool = False


# ── Migration State ──────────────────────────────────────────────────

@dataclass
class MigrationProgress:
    tables_total: int = 0
    tables_schema_created: int = 0
    tables_snapshot_started: int = 0
    tables_snapshot_complete: int = 0
    tables_fk_applied: int = 0
    tables_verified: int = 0
    current_table: str = ""
    events_per_second: float = 0.0
    bytes_transferred: int = 0
    errors: list[str] = field(default_factory=list)


@dataclass
class MigrationEvent:
    timestamp: str
    level: str  # info, warn, error, success
    message: str
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class MigrationState:
    migration_id: str
    phase: MigrationPhase = MigrationPhase.CREATED
    created_at: str = ""
    updated_at: str = ""

    # Connection info (credentials stored transiently, not persisted to disk)
    mysql_host: str = ""
    mysql_port: int = 3306
    mysql_username: str = ""
    mysql_password: str = ""
    pg_host: str = ""
    pg_port: int = 5432
    pg_username: str = ""
    pg_password: str = ""
    pg_database: str = ""

    # Discovery results (serializable summaries)
    source_cluster: dict[str, Any] = field(default_factory=dict)
    target_cluster: dict[str, Any] = field(default_factory=dict)

    # User selections
    selected_databases: list[str] = field(default_factory=list)
    database_mappings: list[dict[str, Any]] = field(default_factory=list)
    designated_keys: dict[str, list[str]] = field(default_factory=dict)
    warnings_acknowledged: bool = False

    # Reports (serializable)
    compatibility_report: dict[str, Any] = field(default_factory=dict)
    sizing_report: dict[str, Any] = field(default_factory=dict)

    # Generated artifacts
    ddl_statements: list[str] = field(default_factory=list)
    connector_configs: list[dict[str, Any]] = field(default_factory=list)

    # Progress tracking
    progress: dict[str, Any] = field(default_factory=dict)
    events: list[dict[str, Any]] = field(default_factory=list)

    error_message: str = ""

    def add_event(self, level: str, message: str, details: dict | None = None) -> None:
        self.events.append({
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": level,
            "message": message,
            "details": details or {},
        })
        if len(self.events) > 500:
            self.events = self.events[-500:]
        self.updated_at = datetime.utcnow().isoformat() + "Z"

    def to_dict(self) -> dict:
        d = asdict(self)
        d["phase"] = self.phase.value
        # Strip credentials before serialization
        d.pop("mysql_password", None)
        d.pop("pg_password", None)
        return d

    @classmethod
    def from_dict(cls, data: dict) -> MigrationState:
        data = dict(data)
        if "phase" in data:
            data["phase"] = MigrationPhase(data["phase"])
        # Passwords won't be in persisted data
        data.setdefault("mysql_password", "")
        data.setdefault("pg_password", "")
        known_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in data.items() if k in known_fields}
        return cls(**filtered)


# ── Live Comparison Models ───────────────────────────────────────────

@dataclass
class TableComparison:
    database: str
    table: str
    source_row_count: int
    target_row_count: int
    in_sync: bool
    delta: int
    source_sample: list[dict[str, Any]] = field(default_factory=list)
    target_sample: list[dict[str, Any]] = field(default_factory=list)
    column_comparison: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class LiveComparisonResult:
    timestamp: str
    databases: list[dict[str, Any]] = field(default_factory=list)
    total_source_rows: int = 0
    total_target_rows: int = 0
    total_delta: int = 0
    fully_synced_tables: int = 0
    total_tables: int = 0
    tables: list[TableComparison] = field(default_factory=list)
