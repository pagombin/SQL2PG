"""PostgreSQL DDL generation from discovered MySQL schema."""

from __future__ import annotations

from .compatibility import map_column_type
from .discovery import build_dependency_order
from .models import (
    CompatibilityReport,
    DiscoveredColumn,
    DiscoveredDatabase,
    DiscoveredIndex,
    DiscoveredTable,
    EnumTypeDef,
    ForeignKey,
    Severity,
    TriggerDef,
)


def _quote(identifier: str) -> str:
    return f'"{identifier}"'


def _map_default(col: DiscoveredColumn, pg_type: str) -> str | None:
    """Translate a MySQL column default to PostgreSQL syntax."""
    if col.is_auto_increment:
        return None  # handled by SERIAL/IDENTITY
    if col.column_default is None:
        return None
    val = col.column_default

    if val.upper() == "CURRENT_TIMESTAMP":
        return "CURRENT_TIMESTAMP"
    if val.upper() == "NULL":
        return "NULL"

    if pg_type == "BOOLEAN":
        if val in ("1", "true", "TRUE", "b'1'"):
            return "TRUE"
        if val in ("0", "false", "FALSE", "b'0'"):
            return "FALSE"

    if "INT" in pg_type.upper() or "NUMERIC" in pg_type.upper() or "REAL" in pg_type.upper() or "DOUBLE" in pg_type.upper():
        try:
            float(val)
            return val
        except (ValueError, TypeError):
            pass

    escaped = val.replace("'", "''")
    return f"'{escaped}'"


# ── ENUM Type Creation ────────────────────────────────────────────────

def generate_enum_ddl(enum_def: EnumTypeDef) -> str:
    values = ", ".join(f"'{v}'" for v in enum_def.values)
    return f"CREATE TYPE {_quote(enum_def.type_name)} AS ENUM ({values});"


# ── Trigger Functions for ON UPDATE CURRENT_TIMESTAMP ────────────────

def generate_trigger_function_ddl(trig: TriggerDef) -> str:
    return (
        f"CREATE OR REPLACE FUNCTION {_quote(trig.function_name)}()\n"
        f"RETURNS TRIGGER AS $$\n"
        f"BEGIN\n"
        f"  NEW.{_quote(trig.column_name)} = CURRENT_TIMESTAMP;\n"
        f"  RETURN NEW;\n"
        f"END;\n"
        f"$$ LANGUAGE plpgsql;"
    )


def generate_trigger_ddl(trig: TriggerDef) -> str:
    return (
        f"CREATE TRIGGER {_quote(trig.trigger_name)}\n"
        f"  BEFORE UPDATE ON {_quote(trig.table_name)}\n"
        f"  FOR EACH ROW\n"
        f"  EXECUTE FUNCTION {_quote(trig.function_name)}();"
    )


# ── Table DDL Generation ─────────────────────────────────────────────

def generate_table_ddl(
    table: DiscoveredTable,
    db_name: str,
    report: CompatibilityReport,
    has_postgis: bool = False,
) -> str:
    """Generate CREATE TABLE DDL for a single table."""
    lines: list[str] = []

    for col in sorted(table.columns, key=lambda c: c.ordinal_position):
        key = f"{db_name}.{table.name}.{col.name}"
        mapping = report.column_mappings.get(key)
        if not mapping or mapping.severity == Severity.BLOCK:
            continue

        pg_type = mapping.pg_type

        # Auto-increment -> GENERATED ALWAYS AS IDENTITY
        if col.is_auto_increment:
            if pg_type in ("BIGINT",):
                pg_type = "BIGINT GENERATED ALWAYS AS IDENTITY"
            elif pg_type in ("SMALLINT",):
                pg_type = "SMALLINT GENERATED ALWAYS AS IDENTITY"
            else:
                pg_type = "INTEGER GENERATED ALWAYS AS IDENTITY"

        parts = [f"  {_quote(col.name)} {pg_type}"]

        if not col.is_auto_increment:
            if not col.is_nullable:
                parts.append("NOT NULL")

            default = _map_default(col, pg_type)
            if default is not None:
                parts.append(f"DEFAULT {default}")

        lines.append(" ".join(parts))

    # Primary key constraint
    if table.primary_key_columns:
        pk_cols = ", ".join(_quote(c) for c in table.primary_key_columns)
        lines.append(f"  PRIMARY KEY ({pk_cols})")

    body = ",\n".join(lines)
    return f"CREATE TABLE IF NOT EXISTS {_quote(table.name)} (\n{body}\n);"


# ── Index DDL ────────────────────────────────────────────────────────

def generate_index_ddl(table: DiscoveredTable) -> list[str]:
    """Generate CREATE INDEX statements for non-PK indexes."""
    statements: list[str] = []
    for idx in table.indexes:
        if idx.is_primary:
            continue
        if idx.index_type == "FULLTEXT":
            continue  # PostgreSQL uses GIN indexes with tsvector; skip for CDC migration
        if idx.index_type == "SPATIAL":
            continue  # Requires PostGIS GiST index; skip

        cols = []
        for ic in sorted(idx.columns, key=lambda c: c.seq_in_index):
            col_ref = _quote(ic.column_name)
            if ic.sub_part:
                # PostgreSQL doesn't support prefix indexes; use expression index
                col_ref = f"LEFT({_quote(ic.column_name)}, {ic.sub_part})"
            cols.append(col_ref)

        unique = "UNIQUE " if idx.is_unique else ""
        col_list = ", ".join(cols)
        idx_name = _quote(f"idx_{table.name}_{idx.name}")
        statements.append(
            f"CREATE {unique}INDEX IF NOT EXISTS {idx_name} "
            f"ON {_quote(table.name)} ({col_list});"
        )
    return statements


# ── Foreign Key DDL (deferred) ───────────────────────────────────────

def generate_fk_ddl(fk: ForeignKey) -> str:
    """Generate ALTER TABLE ADD CONSTRAINT for a foreign key."""
    cols = ", ".join(_quote(c) for c in fk.columns)
    ref_cols = ", ".join(_quote(c) for c in fk.referenced_columns)
    on_delete = fk.on_delete if fk.on_delete != "RESTRICT" else "NO ACTION"
    on_update = fk.on_update if fk.on_update != "RESTRICT" else "NO ACTION"

    return (
        f"ALTER TABLE {_quote(fk.table_name)} "
        f"ADD CONSTRAINT {_quote(fk.constraint_name)} "
        f"FOREIGN KEY ({cols}) "
        f"REFERENCES {_quote(fk.referenced_table)} ({ref_cols}) "
        f"ON DELETE {on_delete} ON UPDATE {on_update};"
    )


# ── Full DDL Generation for a Database ───────────────────────────────

def generate_full_ddl(
    db: DiscoveredDatabase,
    report: CompatibilityReport,
    has_postgis: bool = False,
    create_database: bool = False,
    target_db_name: str | None = None,
) -> list[str]:
    """Generate complete DDL for migrating a MySQL database to PostgreSQL.

    Returns an ordered list of DDL statements:
    1. CREATE DATABASE (optional)
    2. CREATE TYPE (enums)
    3. CREATE TABLE (topologically ordered, no FKs)
    4. CREATE INDEX
    5. Trigger functions + triggers (ON UPDATE CURRENT_TIMESTAMP)
    6. ALTER TABLE ADD CONSTRAINT FOREIGN KEY (deferred)
    """
    ddl: list[str] = []
    pg_db_name = target_db_name or db.name

    # 1. Database creation
    if create_database:
        ddl.append(f"-- Database: {pg_db_name}")
        ddl.append(f'CREATE DATABASE "{pg_db_name}";')
        ddl.append("")

    # 2. ENUM types
    db_enums = [e for e in report.enum_types_needed if e.database == db.name]
    if db_enums:
        ddl.append("-- ENUM types")
        for e in db_enums:
            ddl.append(generate_enum_ddl(e))
        ddl.append("")

    # 3. Tables in dependency order
    table_order, cycles = build_dependency_order(db.tables)
    table_map = {t.name: t for t in db.tables}

    if cycles:
        cycle_tables = set()
        for cycle in cycles:
            cycle_tables.update(cycle)
        ddl.append(f"-- NOTE: Circular FK dependencies detected among: {', '.join(sorted(cycle_tables))}")
        ddl.append("-- All FK constraints will be applied after data loading.")
        ddl.append("")

    ddl.append("-- Tables")
    for tname in table_order:
        if tname not in table_map:
            continue
        table = table_map[tname]
        ddl.append(generate_table_ddl(table, db.name, report, has_postgis))
        ddl.append("")

    # 4. Indexes
    index_statements: list[str] = []
    for tname in table_order:
        if tname not in table_map:
            continue
        idx_stmts = generate_index_ddl(table_map[tname])
        index_statements.extend(idx_stmts)

    if index_statements:
        ddl.append("-- Indexes")
        ddl.extend(index_statements)
        ddl.append("")

    # 5. Trigger functions for ON UPDATE CURRENT_TIMESTAMP
    db_triggers = [t for t in report.triggers_needed if t.database == db.name]
    if db_triggers:
        ddl.append("-- Trigger functions (ON UPDATE CURRENT_TIMESTAMP equivalent)")
        for trig in db_triggers:
            ddl.append(generate_trigger_function_ddl(trig))
            ddl.append(generate_trigger_ddl(trig))
        ddl.append("")

    # 6. Foreign keys (deferred)
    fk_statements: list[str] = []
    for tname in table_order:
        if tname not in table_map:
            continue
        for fk in table_map[tname].foreign_keys:
            if fk.referenced_schema == db.name:
                fk_statements.append(generate_fk_ddl(fk))

    if fk_statements:
        ddl.append("-- Foreign key constraints (applied after data loading)")
        ddl.extend(fk_statements)
        ddl.append("")

    return ddl


def execute_ddl(
    host: str,
    port: int,
    username: str,
    password: str,
    database: str,
    statements: list[str],
    stop_on_error: bool = False,
) -> list[dict]:
    """Execute DDL statements on PostgreSQL. Returns results per statement."""
    import psycopg2

    results: list[dict] = []
    conn = psycopg2.connect(
        host=host, port=port, user=username, password=password,
        dbname=database, sslmode="require",
    )
    conn.autocommit = True
    cursor = conn.cursor()

    for stmt in statements:
        stmt = stmt.strip()
        if not stmt or stmt.startswith("--"):
            continue
        try:
            cursor.execute(stmt)
            results.append({"statement": stmt[:120], "status": "ok"})
        except psycopg2.Error as e:
            error_msg = str(e).strip()
            results.append({"statement": stmt[:120], "status": "error", "error": error_msg})
            if stop_on_error:
                break

    cursor.close()
    conn.close()
    return results


def execute_fk_constraints(
    host: str,
    port: int,
    username: str,
    password: str,
    database: str,
    db_schema: DiscoveredDatabase,
) -> list[dict]:
    """Execute FK ALTER TABLE statements after data loading."""
    results: list[dict] = []
    import psycopg2

    conn = psycopg2.connect(
        host=host, port=port, user=username, password=password,
        dbname=database, sslmode="require",
    )
    conn.autocommit = True
    cursor = conn.cursor()

    for table in db_schema.tables:
        for fk in table.foreign_keys:
            if fk.referenced_schema != db_schema.name:
                continue
            stmt = generate_fk_ddl(fk)
            try:
                cursor.execute(stmt)
                results.append({"constraint": fk.constraint_name, "table": fk.table_name, "status": "ok"})
            except psycopg2.Error as e:
                results.append({
                    "constraint": fk.constraint_name,
                    "table": fk.table_name,
                    "status": "error",
                    "error": str(e).strip(),
                })

    cursor.close()
    conn.close()
    return results
