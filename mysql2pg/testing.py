"""Functionality testing: create data in MySQL, verify replication to PostgreSQL."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from .config import AppConfig, MySQLSource, DatabaseSpec, PostgreSQLTarget


@dataclass
class TestResult:
    source_name: str
    database_name: str
    table_name: str
    step: str
    passed: bool
    message: str
    details: Any = None


@dataclass
class TestSuite:
    results: list[TestResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results)

    @property
    def summary(self) -> str:
        total = len(self.results)
        passed = sum(1 for r in self.results if r.passed)
        return f"{passed}/{total} tests passed"

    def add(self, result: TestResult) -> None:
        self.results.append(result)


TEST_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS customers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
)
"""

TEST_INSERT_SQL = """
INSERT INTO customers (first_name, last_name, email) VALUES
    ('Alice', 'Smith', 'alice@example.com'),
    ('Bob', 'Jones', 'bob@example.com'),
    ('Charlie', 'Brown', 'charlie@example.com')
"""

TEST_UPDATE_SQL = """
UPDATE customers SET email = 'alice.new@example.com' WHERE first_name = 'Alice'
"""


def _get_mysql_connection(source: MySQLSource, database: str | None = None):
    """Create a MySQL connection."""
    import mysql.connector
    conn_kwargs = {
        "host": source.host,
        "port": source.port,
        "user": source.username,
        "password": source.password,
        "ssl_disabled": False,
    }
    if database:
        conn_kwargs["database"] = database
    return mysql.connector.connect(**conn_kwargs)


def _get_pg_connection(pg: PostgreSQLTarget):
    """Create a PostgreSQL connection."""
    import psycopg2
    return psycopg2.connect(
        host=pg.host,
        port=pg.port,
        user=pg.username,
        password=pg.password,
        dbname=pg.database,
        sslmode="require",
    )


def setup_mysql_test_database(source: MySQLSource, db_name: str = "inventory") -> TestResult:
    """Create the test database and table in MySQL, insert sample data."""
    conn = None
    try:
        conn = _get_mysql_connection(source)
        cursor = conn.cursor()

        cursor.execute(
            f"CREATE DATABASE IF NOT EXISTS `{db_name}` "
            "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
        )
        cursor.execute(f"USE `{db_name}`")
        cursor.execute(TEST_TABLE_DDL)
        cursor.execute(TEST_INSERT_SQL)
        conn.commit()

        cursor.execute("SELECT COUNT(*) FROM customers")
        count = cursor.fetchone()[0]

        return TestResult(
            source_name=source.name,
            database_name=db_name,
            table_name="customers",
            step="mysql_setup",
            passed=count == 3,
            message=f"Created database '{db_name}', table 'customers', inserted {count} rows",
            details={"row_count": count},
        )
    except Exception as e:
        return TestResult(
            source_name=source.name,
            database_name=db_name,
            table_name="customers",
            step="mysql_setup",
            passed=False,
            message=f"MySQL setup failed: {e}",
        )
    finally:
        if conn:
            conn.close()


def verify_pg_initial_replication(
    pg: PostgreSQLTarget,
    table_name: str = "customers",
    expected_count: int = 3,
    timeout: int = 120,
    poll_interval: int = 5,
) -> TestResult:
    """Verify that data was replicated to PostgreSQL."""
    import psycopg2
    deadline = time.time() + timeout
    last_error: str = ""
    actual_count = 0

    while time.time() < deadline:
        try:
            conn = _get_pg_connection(pg)
            conn.autocommit = True
            cursor = conn.cursor()

            cursor.execute(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = %s)",
                (table_name,),
            )
            table_exists = cursor.fetchone()[0]

            if not table_exists:
                last_error = f"Table '{table_name}' does not exist yet"
                cursor.close()
                conn.close()
                time.sleep(poll_interval)
                continue

            cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
            actual_count = cursor.fetchone()[0]
            cursor.close()
            conn.close()

            if actual_count >= expected_count:
                return TestResult(
                    source_name="",
                    database_name=pg.database,
                    table_name=table_name,
                    step="pg_verify_initial",
                    passed=True,
                    message=f"Found {actual_count} rows in '{table_name}' (expected {expected_count})",
                    details={"row_count": actual_count},
                )
            last_error = f"Only {actual_count}/{expected_count} rows replicated so far"
            time.sleep(poll_interval)

        except psycopg2.Error as e:
            last_error = str(e)
            time.sleep(poll_interval)

    return TestResult(
        source_name="",
        database_name=pg.database,
        table_name=table_name,
        step="pg_verify_initial",
        passed=False,
        message=f"Replication verification timed out after {timeout}s: {last_error}",
        details={"actual_count": actual_count, "expected_count": expected_count},
    )


def update_mysql_record(source: MySQLSource, db_name: str = "inventory") -> TestResult:
    """Update a record in MySQL to test CDC propagation."""
    conn = None
    try:
        conn = _get_mysql_connection(source, database=db_name)
        cursor = conn.cursor()
        cursor.execute(TEST_UPDATE_SQL)
        conn.commit()
        affected = cursor.rowcount

        return TestResult(
            source_name=source.name,
            database_name=db_name,
            table_name="customers",
            step="mysql_update",
            passed=affected > 0,
            message=f"Updated {affected} row(s) in '{db_name}.customers'",
            details={"rows_affected": affected},
        )
    except Exception as e:
        return TestResult(
            source_name=source.name,
            database_name=db_name,
            table_name="customers",
            step="mysql_update",
            passed=False,
            message=f"MySQL update failed: {e}",
        )
    finally:
        if conn:
            conn.close()


def verify_pg_update(
    pg: PostgreSQLTarget,
    table_name: str = "customers",
    expected_email: str = "alice.new@example.com",
    timeout: int = 60,
    poll_interval: int = 5,
) -> TestResult:
    """Verify that an update was replicated to PostgreSQL."""
    import psycopg2
    deadline = time.time() + timeout
    last_error: str = ""

    while time.time() < deadline:
        try:
            conn = _get_pg_connection(pg)
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute(
                f'SELECT email FROM "{table_name}" WHERE first_name = %s',
                ("Alice",),
            )
            row = cursor.fetchone()
            cursor.close()
            conn.close()

            if row and row[0] == expected_email:
                return TestResult(
                    source_name="",
                    database_name=pg.database,
                    table_name=table_name,
                    step="pg_verify_update",
                    passed=True,
                    message=f"Update replicated: Alice's email is now '{row[0]}'",
                    details={"email": row[0]},
                )
            actual = row[0] if row else "NOT FOUND"
            last_error = f"Alice's email is '{actual}', expected '{expected_email}'"
            time.sleep(poll_interval)

        except psycopg2.Error as e:
            last_error = str(e)
            time.sleep(poll_interval)

    return TestResult(
        source_name="",
        database_name=pg.database,
        table_name=table_name,
        step="pg_verify_update",
        passed=False,
        message=f"Update verification timed out: {last_error}",
    )


def cleanup_mysql_test_data(source: MySQLSource, db_name: str = "inventory") -> TestResult:
    """Clean up test data from MySQL."""
    conn = None
    try:
        conn = _get_mysql_connection(source, database=db_name)
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS customers")
        conn.commit()
        return TestResult(
            source_name=source.name,
            database_name=db_name,
            table_name="customers",
            step="mysql_cleanup",
            passed=True,
            message=f"Cleaned up test data from '{db_name}'",
        )
    except Exception as e:
        return TestResult(
            source_name=source.name,
            database_name=db_name,
            table_name="customers",
            step="mysql_cleanup",
            passed=False,
            message=f"MySQL cleanup failed: {e}",
        )
    finally:
        if conn:
            conn.close()


def cleanup_pg_test_data(pg: PostgreSQLTarget, table_name: str = "customers") -> TestResult:
    """Clean up test data from PostgreSQL."""
    conn = None
    try:
        conn = _get_pg_connection(pg)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        return TestResult(
            source_name="",
            database_name=pg.database,
            table_name=table_name,
            step="pg_cleanup",
            passed=True,
            message=f"Cleaned up table '{table_name}' from PostgreSQL",
        )
    except Exception as e:
        return TestResult(
            source_name="",
            database_name=pg.database,
            table_name=table_name,
            step="pg_cleanup",
            passed=False,
            message=f"PostgreSQL cleanup failed: {e}",
        )
    finally:
        if conn:
            conn.close()


def run_full_test(
    source: MySQLSource,
    pg: PostgreSQLTarget,
    db_name: str = "inventory",
    table_name: str = "customers",
    replication_timeout: int = 120,
) -> TestSuite:
    """
    Run the complete functionality test for a single source database:
    1. Create database & insert data in MySQL
    2. Wait for and verify initial replication to PostgreSQL
    3. Update a record in MySQL
    4. Verify the update propagated to PostgreSQL
    """
    suite = TestSuite()

    result = setup_mysql_test_database(source, db_name)
    suite.add(result)
    if not result.passed:
        return suite

    result = verify_pg_initial_replication(
        pg, table_name=table_name, timeout=replication_timeout
    )
    suite.add(result)
    if not result.passed:
        return suite

    result = update_mysql_record(source, db_name)
    suite.add(result)
    if not result.passed:
        return suite

    result = verify_pg_update(pg, table_name=table_name)
    suite.add(result)

    return suite
