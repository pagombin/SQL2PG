"""Post-migration verification and live data comparison between source and target."""

from __future__ import annotations

import json
import time
from dataclasses import asdict
from datetime import datetime
from typing import Any

from .models import (
    DiscoveredCluster,
    DiscoveredDatabase,
    LiveComparisonResult,
    TableComparison,
)


def _mysql_conn(host, port, user, password, database):
    import mysql.connector
    return mysql.connector.connect(
        host=host, port=port, user=user, password=password,
        database=database, ssl_disabled=False, connection_timeout=30,
    )


def _pg_conn(host, port, user, password, database):
    import psycopg2
    return psycopg2.connect(
        host=host, port=port, user=user, password=password,
        dbname=database, sslmode="require", connect_timeout=30,
    )


def _pg_sql():
    from psycopg2 import sql
    return sql


# ── Row Count Verification ───────────────────────────────────────────

def verify_row_counts(
    mysql_host: str, mysql_port: int, mysql_user: str, mysql_password: str,
    pg_host: str, pg_port: int, pg_user: str, pg_password: str,
    database_mappings: list[dict],
    tables: list[str] | None = None,
) -> list[dict]:
    """Compare row counts between MySQL source and PostgreSQL target.

    Args:
        database_mappings: list of {"source_db": ..., "target_db": ...}
        tables: optional filter; if None, compare all tables in each DB.

    Returns:
        List of per-table comparison dicts.
    """
    results: list[dict] = []

    for mapping in database_mappings:
        src_db = mapping["source_db"]
        tgt_db = mapping["target_db"]

        try:
            my_conn = _mysql_conn(mysql_host, mysql_port, mysql_user, mysql_password, src_db)
            my_cursor = my_conn.cursor()
        except Exception as e:
            results.append({
                "database": src_db, "table": "--",
                "status": "error", "error": f"MySQL connection failed: {e}",
            })
            continue

        try:
            pg = _pg_conn(pg_host, pg_port, pg_user, pg_password, tgt_db)
            pg.autocommit = True
            pg_cursor = pg.cursor()
        except Exception as e:
            my_cursor.close()
            my_conn.close()
            results.append({
                "database": src_db, "table": "--",
                "status": "error", "error": f"PostgreSQL connection failed: {e}",
            })
            continue

        # Get MySQL tables
        my_cursor.execute(
            "SELECT TABLE_NAME FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'",
            (src_db,),
        )
        mysql_tables = [row[0] for row in my_cursor.fetchall()]

        if tables:
            mysql_tables = [t for t in mysql_tables if t in tables]

        for tname in mysql_tables:
            try:
                my_cursor.execute(f"SELECT COUNT(*) FROM `{src_db}`.`{tname}`")
                src_count = my_cursor.fetchone()[0]
            except Exception:
                src_count = -1

            try:
                sql = _pg_sql()
                pg_cursor.execute(
                    "SELECT EXISTS(SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = 'public' AND table_name = %s)",
                    (tname,),
                )
                exists = pg_cursor.fetchone()[0]
                if exists:
                    pg_cursor.execute(sql.SQL('SELECT COUNT(*) FROM {}').format(sql.Identifier(tname)))
                    tgt_count = pg_cursor.fetchone()[0]
                else:
                    tgt_count = -1
            except Exception:
                tgt_count = -1

            delta = abs(src_count - tgt_count) if src_count >= 0 and tgt_count >= 0 else -1
            in_sync = src_count == tgt_count and src_count >= 0

            results.append({
                "database": src_db,
                "target_database": tgt_db,
                "table": tname,
                "source_rows": src_count,
                "target_rows": tgt_count,
                "delta": delta,
                "in_sync": in_sync,
                "status": "ok" if in_sync else ("behind" if tgt_count < src_count else "ahead"),
            })

        my_cursor.close()
        my_conn.close()
        pg_cursor.close()
        pg.close()

    return results


# ── Schema Comparison ─────────────────────────────────────────────────

def verify_schema_match(
    pg_host: str, pg_port: int, pg_user: str, pg_password: str,
    database: str,
    expected_tables: list[str],
) -> list[dict]:
    """Verify that expected tables exist on the PostgreSQL target."""
    results: list[dict] = []
    try:
        conn = _pg_conn(pg_host, pg_port, pg_user, pg_password, database)
        conn.autocommit = True
        cursor = conn.cursor()

        for tname in expected_tables:
            cursor.execute(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = %s)",
                (tname,),
            )
            exists = cursor.fetchone()[0]

            col_count = 0
            if exists:
                cursor.execute(
                    "SELECT COUNT(*) FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND table_name = %s",
                    (tname,),
                )
                col_count = cursor.fetchone()[0]

            results.append({
                "table": tname,
                "exists": exists,
                "column_count": col_count,
                "status": "ok" if exists else "missing",
            })

        cursor.close()
        conn.close()
    except Exception as e:
        results.append({"table": "--", "exists": False, "status": "error", "error": str(e)})

    return results


# ── FK Integrity Check ───────────────────────────────────────────────

def verify_fk_integrity(
    pg_host: str, pg_port: int, pg_user: str, pg_password: str,
    database: str,
) -> list[dict]:
    """Check for FK constraint violations on the PostgreSQL target."""
    results: list[dict] = []
    try:
        conn = _pg_conn(pg_host, pg_port, pg_user, pg_password, database)
        conn.autocommit = True
        cursor = conn.cursor()

        # Get all FK constraints
        cursor.execute("""
            SELECT
                tc.constraint_name,
                tc.table_name,
                kcu.column_name,
                ccu.table_name AS referenced_table,
                ccu.column_name AS referenced_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_schema = 'public'
        """)
        fks = cursor.fetchall()

        sql = _pg_sql()
        for constraint_name, table_name, column_name, ref_table, ref_column in fks:
            try:
                cursor.execute(sql.SQL(
                    'SELECT COUNT(*) FROM {} t'
                    ' LEFT JOIN {} r ON t.{} = r.{}'
                    ' WHERE t.{} IS NOT NULL AND r.{} IS NULL'
                ).format(
                    sql.Identifier(table_name),
                    sql.Identifier(ref_table),
                    sql.Identifier(column_name),
                    sql.Identifier(ref_column),
                    sql.Identifier(column_name),
                    sql.Identifier(ref_column),
                ))
                orphan_count = cursor.fetchone()[0]
                results.append({
                    "constraint": constraint_name,
                    "table": table_name,
                    "column": column_name,
                    "referenced_table": ref_table,
                    "referenced_column": ref_column,
                    "orphaned_rows": orphan_count,
                    "status": "ok" if orphan_count == 0 else "violation",
                })
            except Exception as e:
                results.append({
                    "constraint": constraint_name,
                    "table": table_name,
                    "status": "error",
                    "error": str(e),
                })

        cursor.close()
        conn.close()
    except Exception as e:
        results.append({"constraint": "--", "status": "error", "error": str(e)})

    return results


# ── Sample Data Comparison ───────────────────────────────────────────

def compare_sample_data(
    mysql_host: str, mysql_port: int, mysql_user: str, mysql_password: str,
    pg_host: str, pg_port: int, pg_user: str, pg_password: str,
    source_db: str, target_db: str,
    table_name: str,
    pk_columns: list[str],
    sample_size: int = 10,
) -> dict:
    """Compare sample rows between MySQL source and PostgreSQL target.

    Fetches the first N rows (ordered by PK) from both and returns them
    for side-by-side comparison.
    """
    result: dict[str, Any] = {
        "table": table_name,
        "sample_size": sample_size,
        "source_rows": [],
        "target_rows": [],
        "matches": 0,
        "mismatches": 0,
    }

    if not pk_columns:
        result["error"] = "No primary key; cannot order sample"
        return result

    order_by_mysql = ", ".join(f"`{c}`" for c in pk_columns)

    # MySQL sample
    try:
        my_conn = _mysql_conn(mysql_host, mysql_port, mysql_user, mysql_password, source_db)
        my_cursor = my_conn.cursor(dictionary=True)
        my_cursor.execute(f"SELECT * FROM `{table_name}` ORDER BY {order_by_mysql} LIMIT %s", (sample_size,))
        src_rows = my_cursor.fetchall()
        # Convert to JSON-safe
        for row in src_rows:
            for k, v in row.items():
                if isinstance(v, (bytes, bytearray)):
                    row[k] = v.hex()
                elif hasattr(v, "isoformat"):
                    row[k] = v.isoformat()
                elif isinstance(v, set):
                    row[k] = list(v)
        result["source_rows"] = src_rows
        my_cursor.close()
        my_conn.close()
    except Exception as e:
        result["source_error"] = str(e)
        return result

    # PG sample
    try:
        pg = _pg_conn(pg_host, pg_port, pg_user, pg_password, target_db)
        pg.autocommit = True
        pg_cursor = pg.cursor()

        sql = _pg_sql()
        pg_cursor.execute(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name = %s)",
            (table_name,),
        )
        if not pg_cursor.fetchone()[0]:
            result["target_error"] = f"Table '{table_name}' does not exist on target"
            pg_cursor.close()
            pg.close()
            return result

        order_clause = sql.SQL(', ').join(sql.Identifier(c) for c in pk_columns)
        pg_cursor.execute(
            sql.SQL('SELECT * FROM {} ORDER BY {} LIMIT %s').format(
                sql.Identifier(table_name), order_clause,
            ),
            (sample_size,),
        )
        col_names = [desc[0] for desc in pg_cursor.description]
        tgt_rows = []
        for row in pg_cursor.fetchall():
            row_dict = {}
            for i, val in enumerate(row):
                if isinstance(val, (bytes, bytearray)):
                    row_dict[col_names[i]] = val.hex()
                elif hasattr(val, "isoformat"):
                    row_dict[col_names[i]] = val.isoformat()
                else:
                    row_dict[col_names[i]] = val
            tgt_rows.append(row_dict)

        result["target_rows"] = tgt_rows
        pg_cursor.close()
        pg.close()
    except Exception as e:
        result["target_error"] = str(e)
        return result

    # Compare
    matches = 0
    mismatches = 0
    for src, tgt in zip(result["source_rows"], result["target_rows"]):
        src_key = tuple(str(src.get(k)) for k in pk_columns)
        tgt_key = tuple(str(tgt.get(k)) for k in pk_columns)
        if src_key == tgt_key:
            matches += 1
        else:
            mismatches += 1

    result["matches"] = matches
    result["mismatches"] = mismatches

    return result


# ── Live Comparison (Full) ───────────────────────────────────────────

def live_comparison(
    mysql_host: str, mysql_port: int, mysql_user: str, mysql_password: str,
    pg_host: str, pg_port: int, pg_user: str, pg_password: str,
    database_mappings: list[dict],
    cluster: DiscoveredCluster | None = None,
    include_samples: bool = False,
    sample_size: int = 5,
) -> dict:
    """Run a full live comparison across all mapped databases.

    Returns a comprehensive comparison result suitable for the dashboard.
    """
    timestamp = datetime.utcnow().isoformat() + "Z"

    # Row counts
    row_results = verify_row_counts(
        mysql_host, mysql_port, mysql_user, mysql_password,
        pg_host, pg_port, pg_user, pg_password,
        database_mappings,
    )

    total_source = sum(r.get("source_rows", 0) for r in row_results if r.get("source_rows", 0) > 0)
    total_target = sum(r.get("target_rows", 0) for r in row_results if r.get("target_rows", 0) > 0)
    total_delta = abs(total_source - total_target)
    synced = sum(1 for r in row_results if r.get("in_sync"))
    total_tables = len([r for r in row_results if r.get("table") != "--"])

    # Group by database
    db_summaries: list[dict] = []
    db_groups: dict[str, list[dict]] = {}
    for r in row_results:
        db_name = r.get("database", "unknown")
        db_groups.setdefault(db_name, []).append(r)

    for db_name, tables in db_groups.items():
        db_src = sum(t.get("source_rows", 0) for t in tables if t.get("source_rows", 0) > 0)
        db_tgt = sum(t.get("target_rows", 0) for t in tables if t.get("target_rows", 0) > 0)
        db_synced = sum(1 for t in tables if t.get("in_sync"))
        db_summaries.append({
            "database": db_name,
            "target_database": tables[0].get("target_database", db_name) if tables else db_name,
            "source_rows": db_src,
            "target_rows": db_tgt,
            "delta": abs(db_src - db_tgt),
            "tables_total": len(tables),
            "tables_synced": db_synced,
            "tables": tables,
        })

    # Optional: sample data for specific tables
    samples: list[dict] = []
    if include_samples and cluster:
        for mapping in database_mappings:
            src_db = mapping["source_db"]
            tgt_db = mapping["target_db"]
            for db in cluster.databases:
                if db.name != src_db:
                    continue
                for table in db.tables[:5]:  # Limit to first 5 tables per DB
                    if table.primary_key_columns:
                        sample = compare_sample_data(
                            mysql_host, mysql_port, mysql_user, mysql_password,
                            pg_host, pg_port, pg_user, pg_password,
                            src_db, tgt_db, table.name,
                            table.primary_key_columns,
                            sample_size=sample_size,
                        )
                        sample["database"] = src_db
                        samples.append(sample)

    return {
        "timestamp": timestamp,
        "databases": db_summaries,
        "samples": samples,
        "total_source_rows": total_source,
        "total_target_rows": total_target,
        "total_delta": total_delta,
        "fully_synced_tables": synced,
        "total_tables": total_tables,
        "sync_percentage": round((synced / total_tables * 100) if total_tables > 0 else 0, 1),
    }
