"""MySQL-to-PostgreSQL type mapping engine and compatibility analysis."""

from __future__ import annotations

import re
from dataclasses import dataclass

from .models import (
    CompatibilityIssue,
    CompatibilityReport,
    DiscoveredCluster,
    DiscoveredColumn,
    DiscoveredDatabase,
    DiscoveredTable,
    EnumTypeDef,
    Severity,
    TriggerDef,
    TypeMapping,
)


def _enum_type_name(db_name: str, table_name: str, col_name: str) -> str:
    parts = [db_name, table_name, col_name]
    sanitized = "_".join(p.replace("-", "_").lower() for p in parts)
    return f"enum_{sanitized}"


# ── Core Type Mapping ─────────────────────────────────────────────────

_NUMERIC_MAP = {
    "tinyint": ("SMALLINT", Severity.INFO, ""),
    "smallint": ("SMALLINT", Severity.INFO, ""),
    "mediumint": ("INTEGER", Severity.INFO, "Range widens from 3-byte to 4-byte integer"),
    "int": ("INTEGER", Severity.INFO, ""),
    "integer": ("INTEGER", Severity.INFO, ""),
    "bigint": ("BIGINT", Severity.INFO, ""),
    "float": ("REAL", Severity.INFO, ""),
    "double": ("DOUBLE PRECISION", Severity.INFO, ""),
    "decimal": ("NUMERIC", Severity.INFO, ""),
    "numeric": ("NUMERIC", Severity.INFO, ""),
    "year": ("SMALLINT", Severity.WARN, "YEAR type mapped to SMALLINT; loses year-specific validation"),
    "bool": ("BOOLEAN", Severity.INFO, ""),
    "boolean": ("BOOLEAN", Severity.INFO, ""),
}

_STRING_MAP = {
    "char": ("CHAR", Severity.INFO, ""),
    "varchar": ("VARCHAR", Severity.INFO, ""),
    "tinytext": ("TEXT", Severity.INFO, "Length limit removed"),
    "text": ("TEXT", Severity.INFO, ""),
    "mediumtext": ("TEXT", Severity.INFO, "Length limit removed"),
    "longtext": ("TEXT", Severity.INFO, "Length limit removed"),
}

_BINARY_MAP = {
    "binary": ("BYTEA", Severity.INFO, ""),
    "varbinary": ("BYTEA", Severity.INFO, ""),
    "tinyblob": ("BYTEA", Severity.INFO, ""),
    "blob": ("BYTEA", Severity.INFO, ""),
    "mediumblob": ("BYTEA", Severity.INFO, ""),
    "longblob": ("BYTEA", Severity.WARN, "Max size 4GB in MySQL vs 1GB in PostgreSQL"),
}

_DATETIME_MAP = {
    "date": ("DATE", Severity.INFO, ""),
    "datetime": ("TIMESTAMP WITHOUT TIME ZONE", Severity.INFO, ""),
    "timestamp": ("TIMESTAMP WITH TIME ZONE", Severity.INFO, ""),
    "time": ("TIME", Severity.INFO, ""),
}

_JSON_MAP = {
    "json": ("JSONB", Severity.INFO, "Mapped to JSONB for indexing support"),
}

_SPATIAL_TYPES = {
    "geometry", "point", "linestring", "polygon",
    "multipoint", "multilinestring", "multipolygon",
    "geometrycollection",
}


def map_column_type(
    col: DiscoveredColumn,
    db_name: str,
    table_name: str,
    has_postgis: bool = False,
) -> TypeMapping:
    """Map a single MySQL column to its PostgreSQL equivalent."""

    dtype = col.data_type.lower()
    ctype = col.column_type.lower()

    # TINYINT(1) is conventionally boolean
    if dtype == "tinyint" and "(1)" in ctype and not col.is_unsigned:
        return TypeMapping(
            mysql_type=dtype, mysql_column_type=ctype,
            pg_type="BOOLEAN", severity=Severity.INFO,
            note="TINYINT(1) treated as BOOLEAN",
        )

    # BIT: Debezium sends BIT(1) as boolean, BIT(n>1) as byte array
    if dtype == "bit":
        if col.numeric_precision and col.numeric_precision == 1:
            return TypeMapping(
                mysql_type=dtype, mysql_column_type=ctype,
                pg_type="BOOLEAN", severity=Severity.INFO,
                note="BIT(1) mapped to BOOLEAN (Debezium emits as boolean)",
            )
        return TypeMapping(
            mysql_type=dtype, mysql_column_type=ctype,
            pg_type="BYTEA", severity=Severity.WARN,
            note="BIT(n) mapped to BYTEA (Debezium emits as byte array)",
        )

    # ENUM
    if dtype == "enum":
        ename = _enum_type_name(db_name, table_name, col.name)
        return TypeMapping(
            mysql_type=dtype, mysql_column_type=ctype,
            pg_type=ename, severity=Severity.INFO,
            note=f"ENUM mapped to custom type {ename}",
            requires_enum_type=True, enum_type_name=ename,
        )

    # SET
    if dtype == "set":
        return TypeMapping(
            mysql_type=dtype, mysql_column_type=ctype,
            pg_type="TEXT", severity=Severity.WARN,
            note="SET mapped to TEXT; values stored as comma-separated string",
        )

    # Virtual generated columns (PG only supports STORED)
    if col.is_virtual:
        return TypeMapping(
            mysql_type=dtype, mysql_column_type=ctype,
            pg_type="-- UNSUPPORTED --", severity=Severity.BLOCK,
            note="Virtual generated columns have no PostgreSQL equivalent; only STORED is supported",
        )

    # Spatial types
    if dtype in _SPATIAL_TYPES:
        if has_postgis:
            pg_type = dtype.upper()
            return TypeMapping(
                mysql_type=dtype, mysql_column_type=ctype,
                pg_type=f"geometry({pg_type})", severity=Severity.WARN,
                note="Requires PostGIS extension; spatial functions may differ",
            )
        return TypeMapping(
            mysql_type=dtype, mysql_column_type=ctype,
            pg_type="-- REQUIRES POSTGIS --", severity=Severity.BLOCK,
            note="Spatial type requires PostGIS extension which is not available on the target",
        )

    # Numeric types with UNSIGNED handling
    if dtype in _NUMERIC_MAP:
        base_pg, severity, note = _NUMERIC_MAP[dtype]

        if col.is_unsigned:
            if dtype in ("tinyint",):
                pg_type = "SMALLINT"
                note = "TINYINT UNSIGNED mapped to SMALLINT (0-255 fits)"
                severity = Severity.INFO
            elif dtype in ("smallint",):
                pg_type = "INTEGER"
                note = "SMALLINT UNSIGNED (0-65535) widened to INTEGER"
                severity = Severity.WARN
            elif dtype in ("mediumint",):
                pg_type = "INTEGER"
                note = "MEDIUMINT UNSIGNED widened to INTEGER"
                severity = Severity.WARN
            elif dtype in ("int", "integer"):
                pg_type = "BIGINT"
                note = "INT UNSIGNED (0-4294967295) widened to BIGINT"
                severity = Severity.WARN
            elif dtype == "bigint":
                pg_type = "NUMERIC(20,0)"
                note = "BIGINT UNSIGNED (0-18446744073709551615) mapped to NUMERIC(20,0); no native unsigned 64-bit in PostgreSQL"
                severity = Severity.WARN
            else:
                pg_type = base_pg
        else:
            pg_type = base_pg

        if dtype in ("decimal", "numeric") and col.numeric_precision is not None:
            scale = col.numeric_scale or 0
            pg_type = f"NUMERIC({col.numeric_precision},{scale})"

        if dtype in ("char", "varchar") and col.character_maximum_length:
            pg_type = f"{pg_type}({col.character_maximum_length})"

        return TypeMapping(
            mysql_type=dtype, mysql_column_type=ctype,
            pg_type=pg_type, severity=severity, note=note,
        )

    # String types
    if dtype in _STRING_MAP:
        base_pg, severity, note = _STRING_MAP[dtype]
        pg_type = base_pg
        if dtype in ("char", "varchar") and col.character_maximum_length:
            pg_type = f"{base_pg}({col.character_maximum_length})"
        return TypeMapping(
            mysql_type=dtype, mysql_column_type=ctype,
            pg_type=pg_type, severity=severity, note=note,
        )

    # Binary types
    if dtype in _BINARY_MAP:
        pg_type, severity, note = _BINARY_MAP[dtype]
        return TypeMapping(
            mysql_type=dtype, mysql_column_type=ctype,
            pg_type=pg_type, severity=severity, note=note,
        )

    # Datetime types
    if dtype in _DATETIME_MAP:
        pg_type, severity, note = _DATETIME_MAP[dtype]
        return TypeMapping(
            mysql_type=dtype, mysql_column_type=ctype,
            pg_type=pg_type, severity=severity, note=note,
        )

    # JSON
    if dtype in _JSON_MAP:
        pg_type, severity, note = _JSON_MAP[dtype]
        return TypeMapping(
            mysql_type=dtype, mysql_column_type=ctype,
            pg_type=pg_type, severity=severity, note=note,
        )

    # Unknown type
    return TypeMapping(
        mysql_type=dtype, mysql_column_type=ctype,
        pg_type="-- UNKNOWN --", severity=Severity.BLOCK,
        note=f"No known PostgreSQL mapping for MySQL type '{dtype}'",
    )


# ── Full Compatibility Analysis ──────────────────────────────────────

def analyze_compatibility(
    cluster: DiscoveredCluster,
    selected_databases: list[str],
    has_postgis: bool = False,
) -> CompatibilityReport:
    """Run the full compatibility analysis across all selected databases."""
    report = CompatibilityReport()
    seen_enums: set[str] = set()

    for db in cluster.databases:
        if db.name not in selected_databases:
            continue

        for table in db.tables:
            # Tables without primary key
            if not table.has_primary_key:
                report.tables_without_pk.append(f"{db.name}.{table.name}")
                report.warnings.append(CompatibilityIssue(
                    severity=Severity.WARN,
                    database=db.name,
                    table=table.name,
                    column="--",
                    mysql_type="--",
                    pg_type="--",
                    message=(
                        f"Table has no primary key. Debezium CDC requires a key "
                        f"for upsert semantics. Designate key columns or the table "
                        f"will use insert-only mode."
                    ),
                ))

            for col in table.columns:
                mapping = map_column_type(col, db.name, table.name, has_postgis)
                key = f"{db.name}.{table.name}.{col.name}"
                report.column_mappings[key] = mapping

                if mapping.severity == Severity.BLOCK:
                    report.blockers.append(CompatibilityIssue(
                        severity=Severity.BLOCK,
                        database=db.name,
                        table=table.name,
                        column=col.name,
                        mysql_type=col.column_type,
                        pg_type=mapping.pg_type,
                        message=mapping.note,
                    ))
                elif mapping.severity == Severity.WARN:
                    report.warnings.append(CompatibilityIssue(
                        severity=Severity.WARN,
                        database=db.name,
                        table=table.name,
                        column=col.name,
                        mysql_type=col.column_type,
                        pg_type=mapping.pg_type,
                        message=mapping.note,
                    ))
                else:
                    report.info.append(CompatibilityIssue(
                        severity=Severity.INFO,
                        database=db.name,
                        table=table.name,
                        column=col.name,
                        mysql_type=col.column_type,
                        pg_type=mapping.pg_type,
                        message=mapping.note or "Direct mapping",
                    ))

                # Collect ENUM types
                if mapping.requires_enum_type and mapping.enum_type_name not in seen_enums:
                    seen_enums.add(mapping.enum_type_name)
                    report.enum_types_needed.append(EnumTypeDef(
                        type_name=mapping.enum_type_name,
                        values=col.enum_values,
                        database=db.name,
                    ))

                # ON UPDATE CURRENT_TIMESTAMP -> trigger
                if col.on_update == "CURRENT_TIMESTAMP":
                    fname = f"trg_fn_{table.name}_{col.name}_updated_at"
                    tname = f"trg_{table.name}_{col.name}_updated_at"
                    report.triggers_needed.append(TriggerDef(
                        function_name=fname,
                        trigger_name=tname,
                        table_name=table.name,
                        column_name=col.name,
                        database=db.name,
                    ))

        # Views and routines as info
        if db.views:
            report.info.append(CompatibilityIssue(
                severity=Severity.INFO,
                database=db.name, table="--", column="--",
                mysql_type="--", pg_type="--",
                message=f"{len(db.views)} view(s) found; views are not migrated by CDC",
            ))
        if db.routines:
            report.warnings.append(CompatibilityIssue(
                severity=Severity.WARN,
                database=db.name, table="--", column="--",
                mysql_type="--", pg_type="--",
                message=(
                    f"{len(db.routines)} stored routine(s) found; "
                    f"routines require manual conversion to PL/pgSQL"
                ),
            ))

    # Cross-database FK check
    from .discovery import find_cross_database_fks
    selected_dbs = [db for db in cluster.databases if db.name in selected_databases]
    cross_fks = find_cross_database_fks(selected_dbs)
    for fk in cross_fks:
        report.blockers.append(CompatibilityIssue(
            severity=Severity.BLOCK,
            database=fk.table_schema,
            table=fk.table_name,
            column=", ".join(fk.columns),
            mysql_type="FOREIGN KEY",
            pg_type="--",
            message=(
                f"Cross-database foreign key: {fk.table_schema}.{fk.table_name} -> "
                f"{fk.referenced_schema}.{fk.referenced_table}. "
                f"PostgreSQL databases are isolated; cross-database FKs are not possible."
            ),
        ))

    return report


def report_to_dict(report: CompatibilityReport) -> dict:
    """Serialize a CompatibilityReport to a JSON-safe dict."""

    def issue_dict(i: CompatibilityIssue) -> dict:
        return {
            "severity": i.severity.value,
            "database": i.database,
            "table": i.table,
            "column": i.column,
            "mysql_type": i.mysql_type,
            "pg_type": i.pg_type,
            "message": i.message,
        }

    return {
        "can_proceed": report.can_proceed,
        "blockers": [issue_dict(i) for i in report.blockers],
        "warnings": [issue_dict(i) for i in report.warnings],
        "info": [issue_dict(i) for i in report.info],
        "tables_without_pk": report.tables_without_pk,
        "enum_types_needed": [
            {"type_name": e.type_name, "values": e.values, "database": e.database}
            for e in report.enum_types_needed
        ],
        "triggers_needed": [
            {
                "function_name": t.function_name,
                "trigger_name": t.trigger_name,
                "table_name": t.table_name,
                "column_name": t.column_name,
                "database": t.database,
            }
            for t in report.triggers_needed
        ],
        "total_blockers": len(report.blockers),
        "total_warnings": len(report.warnings),
        "total_info": len(report.info),
    }
