"""MySQL and PostgreSQL schema introspection and discovery."""

from __future__ import annotations

import re
from typing import Any

from .models import (
    DiscoveredCluster,
    DiscoveredColumn,
    DiscoveredDatabase,
    DiscoveredIndex,
    DiscoveredRoutine,
    DiscoveredTable,
    DiscoveredTrigger,
    DiscoveredView,
    ForeignKey,
    IndexColumn,
    PgCluster,
    PgColumn,
    PgDatabase,
    PgTable,
)

SYSTEM_DATABASES = {"information_schema", "mysql", "performance_schema", "sys"}


def _parse_enum_values(column_type: str) -> list[str]:
    m = re.match(r"enum\((.+)\)", column_type, re.IGNORECASE)
    if not m:
        return []
    raw = m.group(1)
    return [v.strip("'") for v in re.findall(r"'([^']*)'", raw)]


def _parse_set_values(column_type: str) -> list[str]:
    m = re.match(r"set\((.+)\)", column_type, re.IGNORECASE)
    if not m:
        return []
    raw = m.group(1)
    return [v.strip("'") for v in re.findall(r"'([^']*)'", raw)]


# ── MySQL Discovery ──────────────────────────────────────────────────

def discover_mysql_cluster(
    host: str,
    port: int,
    username: str,
    password: str,
    database_filter: list[str] | None = None,
) -> DiscoveredCluster:
    """Full introspection of a MySQL cluster: databases, tables, columns,
    indexes, foreign keys, triggers, views, routines."""
    import mysql.connector

    conn = mysql.connector.connect(
        host=host, port=port, user=username, password=password,
        ssl_disabled=False, connection_timeout=30,
    )
    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT VERSION() AS v")
    version = cursor.fetchone()["v"]

    cursor.execute("SELECT @@sql_mode AS m")
    sql_mode = cursor.fetchone()["m"]

    cursor.execute("SELECT @@character_set_server AS cs")
    global_charset = cursor.fetchone()["cs"]

    cursor.execute(
        "SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME "
        "FROM information_schema.SCHEMATA "
        "ORDER BY SCHEMA_NAME"
    )
    all_schemas = cursor.fetchall()

    databases: list[DiscoveredDatabase] = []
    for row in all_schemas:
        db_name = row["SCHEMA_NAME"]
        if db_name.lower() in SYSTEM_DATABASES:
            continue
        if database_filter and db_name not in database_filter:
            continue

        db = DiscoveredDatabase(
            name=db_name,
            charset=row["DEFAULT_CHARACTER_SET_NAME"],
            collation=row["DEFAULT_COLLATION_NAME"],
        )

        db.tables = _discover_tables(cursor, db_name)
        _discover_columns(cursor, db_name, db.tables)
        _discover_indexes(cursor, db_name, db.tables)
        _discover_foreign_keys(cursor, db_name, db.tables)
        _discover_triggers(cursor, db_name, db.tables)
        db.views = _discover_views(cursor, db_name)
        db.routines = _discover_routines(cursor, db_name)

        databases.append(db)

    cursor.close()
    conn.close()

    return DiscoveredCluster(
        host=host,
        port=port,
        version=version,
        databases=databases,
        sql_mode=sql_mode,
        global_charset=global_charset,
    )


def _discover_tables(cursor, schema: str) -> list[DiscoveredTable]:
    cursor.execute(
        "SELECT TABLE_NAME, ENGINE, TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH, "
        "AUTO_INCREMENT, TABLE_COLLATION, TABLE_COMMENT "
        "FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE' "
        "ORDER BY TABLE_NAME",
        (schema,),
    )
    tables = []
    for row in cursor.fetchall():
        tables.append(DiscoveredTable(
            schema_name=schema,
            name=row["TABLE_NAME"],
            engine=row["ENGINE"] or "InnoDB",
            row_count=row["TABLE_ROWS"] or 0,
            data_length=row["DATA_LENGTH"] or 0,
            index_length=row["INDEX_LENGTH"] or 0,
            auto_increment_value=row["AUTO_INCREMENT"],
            table_collation=row["TABLE_COLLATION"],
            comment=row["TABLE_COMMENT"] or "",
        ))
    return tables


def _discover_columns(cursor, schema: str, tables: list[DiscoveredTable]) -> None:
    cursor.execute(
        "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, COLUMN_TYPE, "
        "IS_NULLABLE, COLUMN_DEFAULT, EXTRA, CHARACTER_MAXIMUM_LENGTH, "
        "NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, "
        "CHARACTER_SET_NAME, COLLATION_NAME, COLUMN_COMMENT, "
        "GENERATION_EXPRESSION "
        "FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = %s "
        "ORDER BY TABLE_NAME, ORDINAL_POSITION",
        (schema,),
    )
    cols_by_table: dict[str, list[DiscoveredColumn]] = {}
    for row in cursor.fetchall():
        extra = (row.get("EXTRA") or "").lower()
        column_type = row["COLUMN_TYPE"].lower()
        gen_expr = row.get("GENERATION_EXPRESSION") or ""

        col = DiscoveredColumn(
            name=row["COLUMN_NAME"],
            ordinal_position=row["ORDINAL_POSITION"],
            data_type=row["DATA_TYPE"].lower(),
            column_type=column_type,
            is_nullable=row["IS_NULLABLE"] == "YES",
            column_default=row["COLUMN_DEFAULT"],
            is_auto_increment="auto_increment" in extra,
            is_unsigned="unsigned" in column_type,
            is_zerofill="zerofill" in column_type,
            character_maximum_length=row["CHARACTER_MAXIMUM_LENGTH"],
            numeric_precision=row["NUMERIC_PRECISION"],
            numeric_scale=row["NUMERIC_SCALE"],
            datetime_precision=row["DATETIME_PRECISION"],
            charset=row["CHARACTER_SET_NAME"],
            collation=row["COLLATION_NAME"],
            enum_values=_parse_enum_values(column_type) if row["DATA_TYPE"].lower() == "enum" else [],
            set_values=_parse_set_values(column_type) if row["DATA_TYPE"].lower() == "set" else [],
            on_update="CURRENT_TIMESTAMP" if "on update current_timestamp" in extra else None,
            generation_expression=gen_expr if gen_expr else None,
            is_virtual="virtual generated" in extra,
            is_stored="stored generated" in extra,
            comment=row["COLUMN_COMMENT"] or "",
        )
        cols_by_table.setdefault(row["TABLE_NAME"], []).append(col)

    table_map = {t.name: t for t in tables}
    for tname, cols in cols_by_table.items():
        if tname in table_map:
            table_map[tname].columns = cols


def _discover_indexes(cursor, schema: str, tables: list[DiscoveredTable]) -> None:
    cursor.execute(
        "SELECT TABLE_NAME, INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX, "
        "NON_UNIQUE, INDEX_TYPE, SUB_PART, COLLATION AS IDX_COLLATION, "
        "INDEX_COMMENT "
        "FROM information_schema.STATISTICS "
        "WHERE TABLE_SCHEMA = %s "
        "ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX",
        (schema,),
    )
    idx_data: dict[str, dict[str, dict]] = {}  # table -> index_name -> info
    for row in cursor.fetchall():
        tname = row["TABLE_NAME"]
        iname = row["INDEX_NAME"]
        key = f"{tname}.{iname}"
        if key not in idx_data:
            idx_data[key] = {
                "table": tname,
                "name": iname,
                "is_unique": row["NON_UNIQUE"] == 0,
                "index_type": row["INDEX_TYPE"] or "BTREE",
                "is_primary": iname == "PRIMARY",
                "comment": row["INDEX_COMMENT"] or "",
                "columns": [],
            }
        idx_data[key]["columns"].append(IndexColumn(
            column_name=row["COLUMN_NAME"],
            seq_in_index=row["SEQ_IN_INDEX"],
            sub_part=row["SUB_PART"],
            collation=row["IDX_COLLATION"],
        ))

    table_map = {t.name: t for t in tables}
    for info in idx_data.values():
        tname = info["table"]
        if tname not in table_map:
            continue
        idx = DiscoveredIndex(
            name=info["name"],
            columns=info["columns"],
            is_unique=info["is_unique"],
            index_type=info["index_type"],
            is_primary=info["is_primary"],
            comment=info["comment"],
        )
        table_map[tname].indexes.append(idx)
        if idx.is_primary:
            table_map[tname].has_primary_key = True
            table_map[tname].primary_key_columns = [
                c.column_name for c in sorted(idx.columns, key=lambda x: x.seq_in_index)
            ]


def _discover_foreign_keys(cursor, schema: str, tables: list[DiscoveredTable]) -> None:
    cursor.execute(
        "SELECT rc.CONSTRAINT_NAME, rc.TABLE_NAME, rc.REFERENCED_TABLE_NAME, "
        "rc.UPDATE_RULE, rc.DELETE_RULE, "
        "kcu.COLUMN_NAME, kcu.REFERENCED_COLUMN_NAME, kcu.ORDINAL_POSITION, "
        "kcu.REFERENCED_TABLE_SCHEMA "
        "FROM information_schema.REFERENTIAL_CONSTRAINTS rc "
        "JOIN information_schema.KEY_COLUMN_USAGE kcu "
        "  ON rc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA "
        "  AND rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME "
        "  AND rc.TABLE_NAME = kcu.TABLE_NAME "
        "WHERE rc.CONSTRAINT_SCHEMA = %s "
        "ORDER BY rc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION",
        (schema,),
    )

    fk_data: dict[str, dict] = {}
    for row in cursor.fetchall():
        cname = row["CONSTRAINT_NAME"]
        if cname not in fk_data:
            fk_data[cname] = {
                "constraint_name": cname,
                "table_name": row["TABLE_NAME"],
                "referenced_table": row["REFERENCED_TABLE_NAME"],
                "referenced_schema": row["REFERENCED_TABLE_SCHEMA"] or schema,
                "on_update": row["UPDATE_RULE"],
                "on_delete": row["DELETE_RULE"],
                "columns": [],
                "referenced_columns": [],
            }
        fk_data[cname]["columns"].append(row["COLUMN_NAME"])
        fk_data[cname]["referenced_columns"].append(row["REFERENCED_COLUMN_NAME"])

    table_map = {t.name: t for t in tables}
    for info in fk_data.values():
        fk = ForeignKey(
            constraint_name=info["constraint_name"],
            table_schema=schema,
            table_name=info["table_name"],
            columns=info["columns"],
            referenced_schema=info["referenced_schema"],
            referenced_table=info["referenced_table"],
            referenced_columns=info["referenced_columns"],
            on_update=info["on_update"],
            on_delete=info["on_delete"],
        )
        if info["table_name"] in table_map:
            table_map[info["table_name"]].foreign_keys.append(fk)
        if info["referenced_table"] in table_map:
            table_map[info["referenced_table"]].referenced_by.append(fk)


def _discover_triggers(cursor, schema: str, tables: list[DiscoveredTable]) -> None:
    cursor.execute(
        "SELECT TRIGGER_NAME, EVENT_MANIPULATION, ACTION_TIMING, "
        "ACTION_STATEMENT, EVENT_OBJECT_TABLE "
        "FROM information_schema.TRIGGERS "
        "WHERE TRIGGER_SCHEMA = %s",
        (schema,),
    )
    table_map = {t.name: t for t in tables}
    for row in cursor.fetchall():
        tname = row["EVENT_OBJECT_TABLE"]
        trigger = DiscoveredTrigger(
            name=row["TRIGGER_NAME"],
            event=row["EVENT_MANIPULATION"],
            timing=row["ACTION_TIMING"],
            statement=row["ACTION_STATEMENT"] or "",
            table_name=tname,
        )
        if tname in table_map:
            table_map[tname].triggers.append(trigger)


def _discover_views(cursor, schema: str) -> list[DiscoveredView]:
    cursor.execute(
        "SELECT TABLE_NAME, VIEW_DEFINITION, IS_UPDATABLE "
        "FROM information_schema.VIEWS "
        "WHERE TABLE_SCHEMA = %s",
        (schema,),
    )
    return [
        DiscoveredView(
            schema_name=schema,
            name=row["TABLE_NAME"],
            definition=row["VIEW_DEFINITION"] or "",
            is_updatable=row["IS_UPDATABLE"] == "YES",
        )
        for row in cursor.fetchall()
    ]


def _discover_routines(cursor, schema: str) -> list[DiscoveredRoutine]:
    cursor.execute(
        "SELECT ROUTINE_NAME, ROUTINE_TYPE, ROUTINE_DEFINITION, DTD_IDENTIFIER "
        "FROM information_schema.ROUTINES "
        "WHERE ROUTINE_SCHEMA = %s",
        (schema,),
    )
    return [
        DiscoveredRoutine(
            schema_name=schema,
            name=row["ROUTINE_NAME"],
            routine_type=row["ROUTINE_TYPE"],
            definition=row["ROUTINE_DEFINITION"],
            data_type=row["DTD_IDENTIFIER"],
        )
        for row in cursor.fetchall()
    ]


# ── Dependency Graph ─────────────────────────────────────────────────

def build_dependency_order(tables: list[DiscoveredTable]) -> tuple[list[str], list[list[str]]]:
    """Topological sort of tables by FK dependencies.

    Returns:
        (ordered_names, cycles): ordered table names and any detected cycles.
        Tables with no FK dependencies come first. If cycles exist, the
        involved tables are listed in ``cycles`` and the caller must defer
        FK constraints for those tables.
    """
    graph: dict[str, set[str]] = {t.name: set() for t in tables}
    for t in tables:
        for fk in t.foreign_keys:
            if fk.referenced_table in graph and fk.referenced_table != t.name:
                graph[t.name].add(fk.referenced_table)

    ordered: list[str] = []
    visited: set[str] = set()
    in_stack: set[str] = set()
    cycles: list[list[str]] = []

    def visit(node: str, path: list[str]) -> None:
        if node in in_stack:
            cycle_start = path.index(node)
            cycles.append(path[cycle_start:] + [node])
            return
        if node in visited:
            return
        in_stack.add(node)
        path.append(node)
        for dep in sorted(graph.get(node, set())):
            visit(dep, path)
        path.pop()
        in_stack.discard(node)
        visited.add(node)
        ordered.append(node)

    for name in sorted(graph):
        if name not in visited:
            visit(name, [])

    return ordered, cycles


def find_cross_database_fks(databases: list[DiscoveredDatabase]) -> list[ForeignKey]:
    """Find foreign keys that reference tables in a different database."""
    cross_db: list[ForeignKey] = []
    for db in databases:
        for table in db.tables:
            for fk in table.foreign_keys:
                if fk.referenced_schema != db.name:
                    cross_db.append(fk)
    return cross_db


# ── PostgreSQL Discovery ─────────────────────────────────────────────

def discover_pg_cluster(
    host: str,
    port: int,
    username: str,
    password: str,
    database: str = "defaultdb",
) -> PgCluster:
    """Discover PostgreSQL cluster: databases, capabilities, extensions."""
    import psycopg2

    conn = psycopg2.connect(
        host=host, port=port, user=username, password=password,
        dbname=database, sslmode="require",
        connect_timeout=30,
    )
    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute("SELECT version()")
    version = cursor.fetchone()[0]

    cursor.execute(
        "SELECT datname, pg_catalog.pg_get_userbyid(datdba) AS owner, "
        "pg_encoding_to_char(encoding) AS encoding, datcollate, "
        "pg_database_size(datname) AS size_bytes "
        "FROM pg_database "
        "WHERE datistemplate = false "
        "ORDER BY datname"
    )
    databases = [
        PgDatabase(
            name=row[0], owner=row[1], encoding=row[2],
            collation=row[3], size_bytes=row[4],
        )
        for row in cursor.fetchall()
    ]

    cursor.execute("SELECT extname FROM pg_extension")
    extensions = [row[0] for row in cursor.fetchall()]

    cursor.execute(
        "SELECT name FROM pg_available_extensions WHERE name = 'postgis'"
    )
    postgis_available = cursor.fetchone() is not None

    can_create_db = False
    try:
        cursor.execute("SELECT has_database_privilege(%s, 'CREATE')", (username,))
        row = cursor.fetchone()
        if row:
            can_create_db = True
    except Exception:
        pass

    try:
        cursor.execute("SELECT rolcreatedb FROM pg_roles WHERE rolname = %s", (username,))
        row = cursor.fetchone()
        if row and row[0]:
            can_create_db = True
    except Exception:
        pass

    cursor.close()
    conn.close()

    return PgCluster(
        host=host,
        port=port,
        version=version,
        databases=databases,
        available_extensions=extensions,
        has_postgis=postgis_available or "postgis" in extensions,
        can_create_db=can_create_db,
    )


def discover_pg_tables(
    host: str,
    port: int,
    username: str,
    password: str,
    database: str,
) -> list[PgTable]:
    """Discover tables and their columns in a specific PostgreSQL database."""
    import psycopg2

    conn = psycopg2.connect(
        host=host, port=port, user=username, password=password,
        dbname=database, sslmode="require", connect_timeout=30,
    )
    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute(
        "SELECT table_schema, table_name "
        "FROM information_schema.tables "
        "WHERE table_schema = 'public' AND table_type = 'BASE TABLE' "
        "ORDER BY table_name"
    )
    tables: list[PgTable] = []
    for row in cursor.fetchall():
        tables.append(PgTable(schema_name=row[0], name=row[1]))

    for table in tables:
        cursor.execute(
            "SELECT column_name, data_type, is_nullable, column_default, ordinal_position "
            "FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s "
            "ORDER BY ordinal_position",
            (table.schema_name, table.name),
        )
        table.columns = [
            PgColumn(
                name=r[0], data_type=r[1], is_nullable=r[2] == "YES",
                column_default=r[3], ordinal_position=r[4],
            )
            for r in cursor.fetchall()
        ]

        try:
            cursor.execute(f'SELECT COUNT(*) FROM "{table.name}"')
            table.row_count = cursor.fetchone()[0]
        except Exception:
            table.row_count = 0

    cursor.close()
    conn.close()
    return tables


# ── Serialization Helpers ────────────────────────────────────────────

def cluster_to_summary(cluster: DiscoveredCluster) -> dict:
    """Serialize a DiscoveredCluster to a JSON-safe summary dict."""
    dbs = []
    for db in cluster.databases:
        tables = []
        for t in db.tables:
            fks = [
                {
                    "constraint_name": fk.constraint_name,
                    "columns": fk.columns,
                    "referenced_table": fk.referenced_table,
                    "referenced_columns": fk.referenced_columns,
                    "referenced_schema": fk.referenced_schema,
                    "on_update": fk.on_update,
                    "on_delete": fk.on_delete,
                }
                for fk in t.foreign_keys
            ]
            cols = [
                {
                    "name": c.name,
                    "data_type": c.data_type,
                    "column_type": c.column_type,
                    "is_nullable": c.is_nullable,
                    "column_default": c.column_default,
                    "is_auto_increment": c.is_auto_increment,
                    "is_unsigned": c.is_unsigned,
                    "is_zerofill": c.is_zerofill,
                    "character_maximum_length": c.character_maximum_length,
                    "numeric_precision": c.numeric_precision,
                    "numeric_scale": c.numeric_scale,
                    "datetime_precision": c.datetime_precision,
                    "charset": c.charset,
                    "collation": c.collation,
                    "enum_values": c.enum_values,
                    "set_values": c.set_values,
                    "on_update": c.on_update,
                    "generation_expression": c.generation_expression,
                    "is_virtual": c.is_virtual,
                    "is_stored": c.is_stored,
                }
                for c in t.columns
            ]
            indexes = [
                {
                    "name": idx.name,
                    "is_unique": idx.is_unique,
                    "is_primary": idx.is_primary,
                    "index_type": idx.index_type,
                    "columns": [
                        {"column_name": ic.column_name, "sub_part": ic.sub_part}
                        for ic in idx.columns
                    ],
                }
                for idx in t.indexes
            ]
            tables.append({
                "name": t.name,
                "engine": t.engine,
                "row_count": t.row_count,
                "data_length": t.data_length,
                "index_length": t.index_length,
                "auto_increment_value": t.auto_increment_value,
                "has_primary_key": t.has_primary_key,
                "primary_key_columns": t.primary_key_columns,
                "columns": cols,
                "indexes": indexes,
                "foreign_keys": fks,
            })
        dbs.append({
            "name": db.name,
            "charset": db.charset,
            "collation": db.collation,
            "table_count": db.table_count,
            "total_rows": db.total_rows,
            "total_size": db.total_size,
            "tables": tables,
            "views": [{"name": v.name} for v in db.views],
            "routines": [
                {"name": r.name, "type": r.routine_type} for r in db.routines
            ],
        })
    return {
        "host": cluster.host,
        "port": cluster.port,
        "version": cluster.version,
        "sql_mode": cluster.sql_mode,
        "global_charset": cluster.global_charset,
        "databases": dbs,
        "total_tables": cluster.total_tables,
        "total_size": cluster.total_size,
    }


def pg_cluster_to_summary(cluster: PgCluster) -> dict:
    """Serialize a PgCluster to a JSON-safe summary dict."""
    return {
        "host": cluster.host,
        "port": cluster.port,
        "version": cluster.version,
        "databases": [
            {
                "name": db.name,
                "owner": db.owner,
                "encoding": db.encoding,
                "collation": db.collation,
                "size_bytes": db.size_bytes,
            }
            for db in cluster.databases
        ],
        "available_extensions": cluster.available_extensions,
        "has_postgis": cluster.has_postgis,
        "can_create_db": cluster.can_create_db,
    }


def rebuild_cluster_from_summary(data: dict) -> DiscoveredCluster:
    """Rebuild a DiscoveredCluster from a serialized summary dict."""
    databases = []
    for db_data in data.get("databases", []):
        tables = []
        for td in db_data.get("tables", []):
            cols = [
                DiscoveredColumn(
                    name=c["name"],
                    ordinal_position=i,
                    data_type=c["data_type"],
                    column_type=c["column_type"],
                    is_nullable=c["is_nullable"],
                    column_default=c.get("column_default"),
                    is_auto_increment=c.get("is_auto_increment", False),
                    is_unsigned=c.get("is_unsigned", False),
                    is_zerofill=c.get("is_zerofill", False),
                    character_maximum_length=c.get("character_maximum_length"),
                    numeric_precision=c.get("numeric_precision"),
                    numeric_scale=c.get("numeric_scale"),
                    datetime_precision=c.get("datetime_precision"),
                    charset=c.get("charset"),
                    collation=c.get("collation"),
                    enum_values=c.get("enum_values", []),
                    set_values=c.get("set_values", []),
                    on_update=c.get("on_update"),
                    generation_expression=c.get("generation_expression"),
                    is_virtual=c.get("is_virtual", False),
                    is_stored=c.get("is_stored", False),
                )
                for i, c in enumerate(td.get("columns", []), 1)
            ]
            fks = [
                ForeignKey(
                    constraint_name=fk["constraint_name"],
                    table_schema=db_data["name"],
                    table_name=td["name"],
                    columns=fk["columns"],
                    referenced_schema=fk.get("referenced_schema", db_data["name"]),
                    referenced_table=fk["referenced_table"],
                    referenced_columns=fk["referenced_columns"],
                    on_update=fk.get("on_update", "NO ACTION"),
                    on_delete=fk.get("on_delete", "NO ACTION"),
                )
                for fk in td.get("foreign_keys", [])
            ]
            indexes = [
                DiscoveredIndex(
                    name=idx["name"],
                    columns=[
                        IndexColumn(column_name=ic["column_name"], seq_in_index=j, sub_part=ic.get("sub_part"))
                        for j, ic in enumerate(idx.get("columns", []), 1)
                    ],
                    is_unique=idx.get("is_unique", False),
                    index_type=idx.get("index_type", "BTREE"),
                    is_primary=idx.get("is_primary", False),
                )
                for idx in td.get("indexes", [])
            ]
            t = DiscoveredTable(
                schema_name=db_data["name"],
                name=td["name"],
                engine=td.get("engine", "InnoDB"),
                row_count=td.get("row_count", 0),
                data_length=td.get("data_length", 0),
                index_length=td.get("index_length", 0),
                auto_increment_value=td.get("auto_increment_value"),
                table_collation=None,
                comment="",
                columns=cols,
                indexes=indexes,
                foreign_keys=fks,
                primary_key_columns=td.get("primary_key_columns", []),
                has_primary_key=td.get("has_primary_key", False),
            )
            tables.append(t)
        databases.append(DiscoveredDatabase(
            name=db_data["name"],
            charset=db_data.get("charset", "utf8mb4"),
            collation=db_data.get("collation", "utf8mb4_0900_ai_ci"),
            tables=tables,
        ))
    return DiscoveredCluster(
        host=data.get("host", ""),
        port=data.get("port", 3306),
        version=data.get("version", ""),
        databases=databases,
        sql_mode=data.get("sql_mode", ""),
        global_charset=data.get("global_charset", ""),
    )
