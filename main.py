#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MySQL2PG CLI: Automated MySQL-to-PostgreSQL migration via Kafka (Debezium CDC).

Supports multiple MySQL source databases replicating to a single PostgreSQL target
through DigitalOcean Managed Kafka with Debezium CDC and JDBC sink connectors.
"""

from __future__ import annotations

import json
import sys
import time

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn

from mysql2pg import __version__
from mysql2pg.config import load_config, AppConfig
from mysql2pg.aiven import AivenClient, AivenAPIError
from mysql2pg.connectors import (
    build_all_connectors,
    build_source_connector,
    build_sink_connectors_for_source,
    get_all_connector_names,
    ConnectorPlan,
)
from mysql2pg.testing import run_full_test, TestSuite, cleanup_mysql_test_data, cleanup_pg_test_data
from mysql2pg.web import run_server

console = Console()


def _load(config_path: str) -> AppConfig:
    try:
        return load_config(config_path)
    except (FileNotFoundError, ValueError) as e:
        console.print(f"[red]Configuration error:[/red] {e}")
        sys.exit(1)


def _client(config: AppConfig) -> AivenClient:
    return AivenClient(config.aiven.token, config.aiven.project)


def _print_connector_table(plans: list[ConnectorPlan]) -> None:
    table = Table(title="Connector Plan")
    table.add_column("Name", style="cyan")
    table.add_column("Type", style="magenta")
    table.add_column("Source", style="green")
    table.add_column("Database(s)", style="yellow")
    for plan in plans:
        table.add_row(plan.name, plan.connector_type, plan.source_name, plan.database_name)
    console.print(table)


# ── CLI Group ────────────────────────────────────────────────────────

@click.group()
@click.version_option(version=__version__, prog_name="mysql2pg")
@click.option("--config", "-c", default="config.yaml", help="Path to config file")
@click.pass_context
def cli(ctx: click.Context, config: str) -> None:
    """MySQL2PG: Automated MySQL-to-PostgreSQL migration via Kafka CDC."""
    ctx.ensure_object(dict)
    ctx.obj["config_path"] = config


# ── Setup Command ────────────────────────────────────────────────────

@cli.command()
@click.pass_context
def setup(ctx: click.Context) -> None:
    """Enable Kafka Connect and auto-create topics on the Kafka cluster."""
    config = _load(ctx.obj["config_path"])
    client = _client(config)
    kafka_svc = config.kafka.service_name

    console.print(Panel("[bold]Step 1: Setting up Kafka cluster[/bold]"))

    with Progress(SpinnerColumn(), TextColumn("{task.description}"), console=console) as progress:
        task = progress.add_task("Enabling Kafka Connect...", total=None)
        try:
            client.enable_kafka_connect(kafka_svc)
            progress.update(task, description="[green]Kafka Connect enable request sent")
        except AivenAPIError as e:
            progress.update(task, description=f"[red]Failed: {e}")
            sys.exit(1)

        task = progress.add_task("Waiting for Kafka Connect to become active...", total=None)
        if client.wait_for_kafka_connect(kafka_svc, timeout=300):
            progress.update(task, description="[green]Kafka Connect is active")
        else:
            progress.update(task, description="[yellow]Timeout waiting for Kafka Connect (may still be starting)")

        task = progress.add_task("Enabling auto.create.topics...", total=None)
        try:
            client.enable_auto_create_topics(kafka_svc)
            progress.update(task, description="[green]Auto-create topics enabled")
        except AivenAPIError as e:
            progress.update(task, description=f"[red]Failed: {e}")
            sys.exit(1)

    console.print("\n[green bold]Setup complete.[/green bold] Kafka cluster is ready for connectors.")


# ── Plan Command ─────────────────────────────────────────────────────

@cli.command()
@click.pass_context
def plan(ctx: click.Context) -> None:
    """Show the connector deployment plan without making any changes."""
    config = _load(ctx.obj["config_path"])
    plans = build_all_connectors(config)

    console.print(Panel("[bold]Connector Deployment Plan[/bold]"))
    _print_connector_table(plans)

    source_count = sum(1 for p in plans if p.connector_type == "source")
    sink_count = sum(1 for p in plans if p.connector_type == "sink")
    console.print(f"\nTotal: [cyan]{source_count}[/cyan] source + [cyan]{sink_count}[/cyan] sink connectors")


# ── Deploy Command ───────────────────────────────────────────────────

@cli.command()
@click.option("--sources-only", is_flag=True, help="Deploy only source (Debezium) connectors")
@click.option("--sinks-only", is_flag=True, help="Deploy only sink (JDBC) connectors")
@click.option("--wait/--no-wait", default=True, help="Wait for connectors to reach RUNNING state")
@click.option("--timeout", default=300, help="Timeout in seconds when waiting for connectors")
@click.pass_context
def deploy(ctx: click.Context, sources_only: bool, sinks_only: bool, wait: bool, timeout: int) -> None:
    """Deploy source and/or sink Kafka connectors."""
    config = _load(ctx.obj["config_path"])
    client = _client(config)
    kafka_svc = config.kafka.service_name

    all_plans = build_all_connectors(config)
    if sources_only:
        plans = [p for p in all_plans if p.connector_type == "source"]
    elif sinks_only:
        plans = [p for p in all_plans if p.connector_type == "sink"]
    else:
        plans = all_plans

    if not plans:
        console.print("[yellow]No connectors to deploy.[/yellow]")
        return

    console.print(Panel("[bold]Deploying Connectors[/bold]"))
    _print_connector_table(plans)
    console.print()

    deployed: list[str] = []
    failed: list[str] = []

    for plan in plans:
        connector_display = f"[cyan]{plan.name}[/cyan] ({plan.connector_type})"
        try:
            client.create_connector(kafka_svc, plan.config)
            console.print(f"  [green]\u2713[/green] Created {connector_display}")
            deployed.append(plan.name)
        except AivenAPIError as e:
            if e.status_code == 409:
                console.print(f"  [yellow]\u25cb[/yellow] Already exists: {connector_display}")
                deployed.append(plan.name)
            else:
                console.print(f"  [red]\u2717[/red] Failed {connector_display}: {e}")
                failed.append(plan.name)

    if wait and deployed:
        console.print(f"\nWaiting for {len(deployed)} connector(s) to reach RUNNING state...")
        with Progress(SpinnerColumn(), TextColumn("{task.description}"), console=console) as progress:
            for name in deployed:
                task = progress.add_task(f"Waiting for {name}...", total=None)
                status = client.wait_for_connector_running(kafka_svc, name, timeout=timeout)
                state = status.get("status", {}).get("state", "UNKNOWN")
                if state == "RUNNING":
                    progress.update(task, description=f"[green]{name}: RUNNING")
                elif state == "FAILED":
                    trace = status.get("status", {}).get("trace", "")
                    progress.update(task, description=f"[red]{name}: FAILED - {trace[:100]}")
                    failed.append(name)
                else:
                    progress.update(task, description=f"[yellow]{name}: {state}")

    console.print(f"\n[bold]Deployed:[/bold] {len(deployed)}  [bold]Failed:[/bold] {len(failed)}")
    if failed:
        sys.exit(1)


# ── Status Command ───────────────────────────────────────────────────

@cli.command()
@click.pass_context
def status(ctx: click.Context) -> None:
    """Check the status of all deployed connectors."""
    config = _load(ctx.obj["config_path"])
    client = _client(config)
    kafka_svc = config.kafka.service_name

    console.print(Panel("[bold]Connector Status[/bold]"))

    try:
        result = client.list_connectors(kafka_svc)
    except AivenAPIError as e:
        console.print(f"[red]Failed to list connectors: {e}[/red]")
        sys.exit(1)

    connectors = result.get("connectors", [])
    if not connectors:
        console.print("[yellow]No connectors found.[/yellow]")
        return

    table = Table()
    table.add_column("Connector", style="cyan")
    table.add_column("State", style="bold")
    table.add_column("Type")
    table.add_column("Tasks")

    for conn_info in connectors:
        name = conn_info.get("name", "unknown")
        try:
            status_resp = client.get_connector_status(kafka_svc, name)
            state = status_resp.get("status", {}).get("state", "UNKNOWN")
            conn_type = status_resp.get("status", {}).get("type", "?")
            tasks = status_resp.get("status", {}).get("tasks", [])
            task_summary = ", ".join(
                f"{t.get('state', '?')}" for t in tasks
            ) if tasks else "none"
        except AivenAPIError:
            state = "ERROR"
            conn_type = "?"
            task_summary = "unavailable"

        state_color = {
            "RUNNING": "green",
            "PAUSED": "yellow",
            "FAILED": "red",
            "UNASSIGNED": "yellow",
        }.get(state, "white")

        table.add_row(name, f"[{state_color}]{state}[/{state_color}]", conn_type, task_summary)

    console.print(table)


# ── List Command ─────────────────────────────────────────────────────

@cli.command(name="list")
@click.pass_context
def list_connectors(ctx: click.Context) -> None:
    """List all connectors on the Kafka cluster."""
    config = _load(ctx.obj["config_path"])
    client = _client(config)
    kafka_svc = config.kafka.service_name

    try:
        result = client.list_connectors(kafka_svc)
    except AivenAPIError as e:
        console.print(f"[red]Failed: {e}[/red]")
        sys.exit(1)

    connectors = result.get("connectors", [])
    if not connectors:
        console.print("[yellow]No connectors found.[/yellow]")
        return

    console.print(Panel(f"[bold]Connectors on {kafka_svc}[/bold]"))
    for conn_info in connectors:
        name = conn_info.get("name", "unknown")
        connector_config = conn_info.get("config", {})
        cls = connector_config.get("connector.class", "?")
        console.print(f"  [cyan]{name}[/cyan]  ({cls})")


# ── Teardown Command ─────────────────────────────────────────────────

@cli.command()
@click.option("--all", "delete_all", is_flag=True, help="Delete ALL connectors, not just those in config")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompt")
@click.pass_context
def teardown(ctx: click.Context, delete_all: bool, yes: bool) -> None:
    """Delete connectors from the Kafka cluster."""
    config = _load(ctx.obj["config_path"])
    client = _client(config)
    kafka_svc = config.kafka.service_name

    if delete_all:
        try:
            result = client.list_connectors(kafka_svc)
            names = [c.get("name") for c in result.get("connectors", []) if c.get("name")]
        except AivenAPIError as e:
            console.print(f"[red]Failed to list connectors: {e}[/red]")
            sys.exit(1)
    else:
        names = get_all_connector_names(config)

    if not names:
        console.print("[yellow]No connectors to delete.[/yellow]")
        return

    console.print(Panel("[bold red]Teardown Plan[/bold red]"))
    for name in names:
        console.print(f"  [red]\u2022[/red] {name}")

    if not yes:
        if not click.confirm(f"\nDelete {len(names)} connector(s)?"):
            console.print("[yellow]Cancelled.[/yellow]")
            return

    deleted = 0
    for name in names:
        try:
            client.delete_connector(kafka_svc, name)
            console.print(f"  [green]\u2713[/green] Deleted [cyan]{name}[/cyan]")
            deleted += 1
        except AivenAPIError as e:
            if e.status_code == 404:
                console.print(f"  [yellow]\u25cb[/yellow] Not found: [cyan]{name}[/cyan]")
            else:
                console.print(f"  [red]\u2717[/red] Failed to delete [cyan]{name}[/cyan]: {e}")

    console.print(f"\n[bold]Deleted {deleted}/{len(names)} connector(s).[/bold]")


# ── Test Command ─────────────────────────────────────────────────────

@cli.command()
@click.option("--source", "-s", default=None, help="Name of a specific MySQL source to test (default: first source)")
@click.option("--database", "-d", default="inventory", help="Database name to use for testing")
@click.option("--timeout", default=120, help="Replication verification timeout in seconds")
@click.option("--cleanup/--no-cleanup", default=False, help="Clean up test data after testing")
@click.pass_context
def test(ctx: click.Context, source: str | None, database: str, timeout: int, cleanup: bool) -> None:
    """Run the end-to-end functionality test (create data in MySQL, verify in PostgreSQL)."""
    config = _load(ctx.obj["config_path"])

    if source:
        matched = [s for s in config.mysql_sources if s.name == source]
        if not matched:
            available = ", ".join(s.name for s in config.mysql_sources)
            console.print(f"[red]Source '{source}' not found. Available: {available}[/red]")
            sys.exit(1)
        test_source = matched[0]
    else:
        test_source = config.mysql_sources[0]

    pg = config.postgresql_target

    console.print(Panel(f"[bold]Functionality Test: {test_source.name} -> PostgreSQL[/bold]"))
    console.print(f"  MySQL source: [cyan]{test_source.host}:{test_source.port}[/cyan]")
    console.print(f"  PostgreSQL:   [cyan]{pg.host}:{pg.port}/{pg.database}[/cyan]")
    console.print(f"  Test database: [cyan]{database}[/cyan]")
    console.print(f"  Timeout:      [cyan]{timeout}s[/cyan]\n")

    suite = run_full_test(test_source, pg, db_name=database, replication_timeout=timeout)

    table = Table(title="Test Results")
    table.add_column("Step", style="cyan")
    table.add_column("Result", style="bold")
    table.add_column("Message")

    for result in suite.results:
        color = "green" if result.passed else "red"
        symbol = "\u2713" if result.passed else "\u2717"
        table.add_row(
            result.step,
            f"[{color}]{symbol} {'PASS' if result.passed else 'FAIL'}[/{color}]",
            result.message,
        )

    console.print(table)
    console.print(f"\n[bold]{suite.summary}[/bold]")

    if cleanup:
        console.print("\nCleaning up test data...")
        cleanup_mysql_test_data(test_source, database)
        cleanup_pg_test_data(pg, "customers")
        console.print("[green]Cleanup complete.[/green]")

    if not suite.passed:
        sys.exit(1)


# ── Verify Command ───────────────────────────────────────────────────

@cli.command()
@click.pass_context
def verify(ctx: click.Context) -> None:
    """Verify the Kafka cluster setup (Kafka Connect enabled, topics auto-create on)."""
    config = _load(ctx.obj["config_path"])
    client = _client(config)
    kafka_svc = config.kafka.service_name

    console.print(Panel(f"[bold]Verifying Kafka cluster: {kafka_svc}[/bold]"))

    try:
        features = client.get_service_features(kafka_svc)
    except AivenAPIError as e:
        console.print(f"[red]Failed to retrieve service info: {e}[/red]")
        sys.exit(1)

    checks = {
        "kafka_connect": features.get("kafka_connect", False),
        "kafka_connect_service_integration": features.get("kafka_connect_service_integration", False),
    }

    for feature, enabled in checks.items():
        symbol = "\u2713" if enabled else "\u2717"
        color = "green" if enabled else "red"
        console.print(f"  [{color}]{symbol}[/{color}] {feature}: {'enabled' if enabled else 'disabled'}")

    try:
        service_data = client.get_service(kafka_svc)
        user_config = service_data.get("service", {}).get("user_config", {})
        kafka_config = user_config.get("kafka", {})
        auto_create = kafka_config.get("auto_create_topics_enable", False)
    except (AivenAPIError, KeyError):
        auto_create = None

    if auto_create is not None:
        symbol = "\u2713" if auto_create else "\u2717"
        color = "green" if auto_create else "red"
        console.print(f"  [{color}]{symbol}[/{color}] auto_create_topics_enable: {'enabled' if auto_create else 'disabled'}")

    connect_uri = client.get_kafka_connect_uri(kafka_svc)
    if connect_uri:
        console.print(f"\n  Kafka Connect URI: [dim]{connect_uri}[/dim]")


# ── Run Command (Full Pipeline) ─────────────────────────────────────

@cli.command()
@click.option("--skip-setup", is_flag=True, help="Skip Kafka setup (assumes already configured)")
@click.option("--skip-test", is_flag=True, help="Skip functionality test")
@click.option("--timeout", default=300, help="Timeout for connector startup in seconds")
@click.pass_context
def run(ctx: click.Context, skip_setup: bool, skip_test: bool, timeout: int) -> None:
    """Run the full pipeline: setup -> deploy -> verify -> test."""
    config_path = ctx.obj["config_path"]
    config = _load(config_path)

    steps = []
    if not skip_setup:
        steps.append(("Setup", "setup"))
    steps.append(("Deploy", "deploy"))
    steps.append(("Verify", "verify"))
    if not skip_test:
        steps.append(("Test", "test"))

    console.print(Panel("[bold]MySQL2PG Full Pipeline[/bold]"))
    console.print(f"  Sources: [cyan]{len(config.mysql_sources)}[/cyan] MySQL database(s)")
    total_dbs = sum(len(src.databases) for src in config.mysql_sources)
    console.print(f"  Databases: [cyan]{total_dbs}[/cyan] total")
    console.print(f"  Target: [cyan]{config.postgresql_target.host}/{config.postgresql_target.database}[/cyan]")
    console.print(f"  Strategy: [cyan]{config.table_name_strategy}[/cyan]\n")

    if not skip_setup:
        console.rule("[bold]Step 1: Setup[/bold]")
        ctx.invoke(setup)
        console.print()

    console.rule("[bold]Step 2: Deploy Connectors[/bold]")
    ctx.invoke(deploy, timeout=timeout)
    console.print()

    console.rule("[bold]Step 3: Verify[/bold]")
    ctx.invoke(verify)
    console.print()

    if not skip_test:
        console.rule("[bold]Step 4: Functionality Test[/bold]")
        ctx.invoke(test)

    console.print("\n[green bold]Pipeline complete.[/green bold]")


# ── Info Command ─────────────────────────────────────────────────────

@cli.command()
@click.pass_context
def info(ctx: click.Context) -> None:
    """Display configuration summary."""
    config = _load(ctx.obj["config_path"])

    console.print(Panel("[bold]Configuration Summary[/bold]"))

    console.print(f"  Aiven Project: [cyan]{config.aiven.project}[/cyan]")
    console.print(f"  Kafka Service: [cyan]{config.kafka.service_name}[/cyan]")
    console.print(f"  Kafka SSL:     [cyan]{config.kafka.ssl_host}:{config.kafka.ssl_port}[/cyan]")
    console.print(f"  Table Strategy: [cyan]{config.table_name_strategy}[/cyan]")
    console.print()

    table = Table(title="MySQL Sources")
    table.add_column("Name", style="cyan")
    table.add_column("Host")
    table.add_column("Port")
    table.add_column("Server ID")
    table.add_column("Databases")

    for src in config.mysql_sources:
        dbs = ", ".join(
            f"{db.name} ({len(db.tables)} tables)" if db.tables else f"{db.name} (all)"
            for db in src.databases
        )
        table.add_row(src.name, src.host, str(src.port), str(src.server_id), dbs)

    console.print(table)
    console.print()

    pg = config.postgresql_target
    console.print(f"  PostgreSQL Target: [cyan]{pg.host}:{pg.port}/{pg.database}[/cyan]")
    console.print(f"  PostgreSQL User:   [cyan]{pg.username}[/cyan]")


# ── Serve Command (Web UI + HTTPS) ───────────────────────────────────

@cli.command()
@click.option("--host", default="0.0.0.0", help="Bind address")
@click.option("--port", default=8443, type=int, help="HTTPS port")
@click.option("--cert-dir", default="/opt/mysql2pg/certs", help="Directory for TLS certificates")
@click.option("--debug", is_flag=True, help="Enable Flask debug mode")
@click.pass_context
def serve(ctx: click.Context, host: str, port: int, cert_dir: str, debug: bool) -> None:
    """Start the MySQL2PG web dashboard over HTTPS."""
    config_path = ctx.obj["config_path"]
    console.print(Panel("[bold]MySQL2PG Web Server[/bold]"))
    console.print(f"  Config:  [cyan]{config_path}[/cyan]")
    console.print(f"  Listen:  [cyan]https://{host}:{port}[/cyan]")
    console.print(f"  Certs:   [cyan]{cert_dir}[/cyan]\n")
    run_server(
        config_path=config_path,
        host=host,
        port=port,
        cert_dir=cert_dir,
        debug=debug,
    )


if __name__ == "__main__":
    cli()
