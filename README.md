# MySQL2PG

Automated MySQL-to-PostgreSQL migration via Kafka CDC (Change Data Capture).

Supports **multiple MySQL source databases** replicating to a **single PostgreSQL target** through DigitalOcean Managed Kafka using Debezium CDC source connectors and JDBC sink connectors, orchestrated via the Aiven API.

## Architecture

```
┌──────────────┐     ┌─────────────────────┐     ┌──────────────────┐
│ MySQL Source  │────>│ Debezium Source      │     │                  │
│ (instance 1) │     │ Connector            │────>│                  │
└──────────────┘     └─────────────────────┘     │  Kafka Cluster   │
                                                  │  (DO Managed)    │
┌──────────────┐     ┌─────────────────────┐     │                  │
│ MySQL Source  │────>│ Debezium Source      │────>│                  │
│ (instance 2) │     │ Connector            │     │                  │
└──────────────┘     └─────────────────────┘     └────────┬─────────┘
                                                          │
       ┌──────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────┐     ┌──────────────────┐
│ JDBC Sink Connector  │────>│ PostgreSQL Target │
│ (per database)       │     │ (DO Managed)     │
└─────────────────────┘     └──────────────────┘
```

Each MySQL source instance gets its own Debezium source connector. Each source database gets a dedicated JDBC sink connector that writes to the shared PostgreSQL target.

## Prerequisites

- Python 3.10+
- DigitalOcean account with Managed MySQL, PostgreSQL, and Kafka clusters provisioned
- Aiven API token (retrievable from Atlantis or Aiven console)
- Kafka cluster SSL connection details (hostname and port from the **SSL** tab, not SASL)

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

Copy the example configuration and fill in your values:

```bash
cp config.yaml.example config.yaml
```

### Configuration Reference

| Section | Field | Description |
|---------|-------|-------------|
| `aiven.token` | Aiven API bearer token | |
| `aiven.project` | Aiven project name (e.g., `do-user-xxxxxxx-0`) | |
| `kafka.service_name` | Kafka cluster name in DigitalOcean | |
| `kafka.ssl_host` | Kafka SSL hostname (from Connection Details > SSL tab) | |
| `kafka.ssl_port` | Kafka SSL port (from Connection Details > SSL tab) | |
| `mysql_sources[].name` | Unique identifier for this MySQL source | |
| `mysql_sources[].host` | MySQL hostname | |
| `mysql_sources[].port` | MySQL port | |
| `mysql_sources[].username` | MySQL username | |
| `mysql_sources[].password` | MySQL password | |
| `mysql_sources[].server_id` | Unique server ID for Debezium binlog reader | |
| `mysql_sources[].databases[].name` | Database name to replicate | |
| `mysql_sources[].databases[].tables` | Specific tables to replicate (omit for all) | |
| `postgresql_target.host` | PostgreSQL hostname | |
| `postgresql_target.port` | PostgreSQL port | |
| `postgresql_target.username` | PostgreSQL username | |
| `postgresql_target.password` | PostgreSQL password | |
| `postgresql_target.database` | Target database name (default: `defaultdb`) | |
| `table_name_strategy` | `as_is` (original names) or `prefixed` (e.g., `inventory_customers`) | |

### Environment Variable Substitution

Config values can reference environment variables using `${VAR_NAME}` syntax:

```yaml
aiven:
  token: "${AIVEN_TOKEN}"
mysql_sources:
  - name: "prod-mysql"
    password: "${MYSQL_PASSWORD}"
```

## Usage

### Full Pipeline (Recommended)

Run the entire setup, deployment, verification, and test in one command:

```bash
python3 main.py run
```

Options:
- `--skip-setup` — skip Kafka Connect enablement (if already configured)
- `--skip-test` — skip the functionality test
- `--timeout N` — connector startup timeout in seconds (default: 300)

### Individual Commands

#### Setup Kafka Cluster

Enable Kafka Connect and auto-create topics:

```bash
python3 main.py setup
```

#### View Deployment Plan

Preview what connectors will be created without making any changes:

```bash
python3 main.py plan
```

#### Deploy Connectors

Deploy all source and sink connectors:

```bash
python3 main.py deploy
```

Deploy only source (Debezium) connectors:

```bash
python3 main.py deploy --sources-only
```

Deploy only sink (JDBC) connectors:

```bash
python3 main.py deploy --sinks-only
```

#### Check Connector Status

```bash
python3 main.py status
```

#### List Connectors

```bash
python3 main.py list
```

#### Verify Kafka Setup

```bash
python3 main.py verify
```

#### Run Functionality Test

Test CDC replication by inserting data into MySQL and verifying it appears in PostgreSQL:

```bash
python3 main.py test
```

Test a specific source:

```bash
python3 main.py test --source mysql-source-1 --database inventory
```

Clean up test data after testing:

```bash
python3 main.py test --cleanup
```

#### Teardown

Delete all connectors defined in config:

```bash
python3 main.py teardown -y
```

Delete ALL connectors on the Kafka cluster:

```bash
python3 main.py teardown --all -y
```

#### Configuration Info

```bash
python3 main.py info
```

### Using a Custom Config File

All commands accept `-c` / `--config` to specify a different config file:

```bash
python3 main.py -c production.yaml run
```

## Multiple MySQL Sources

The tool is designed to handle multiple MySQL source instances, each potentially containing multiple databases. For each MySQL source:

1. A single **Debezium source connector** is created, covering all databases in that instance via `database.include.list`.
2. A **JDBC sink connector** is created per database, routing topics to the correct PostgreSQL tables.

### Topic Naming

Topics follow the pattern: `mysql_cdc_{source-name}.{database}.{table}`

Example with two sources:
- `mysql_cdc_mysql-source-1.inventory.customers`
- `mysql_cdc_mysql-source-1.inventory.orders`
- `mysql_cdc_mysql-source-2.analytics.events`

### Table Name Collision

When replicating from multiple sources that have tables with the same name, use `table_name_strategy: prefixed` to avoid collisions:

```yaml
table_name_strategy: "prefixed"
```

This produces PostgreSQL table names like `inventory_customers` instead of just `customers`.

## Project Structure

```
├── main.py                  # CLI entry point
├── config.yaml.example      # Configuration template
├── requirements.txt         # Python dependencies
├── mysql2pg/
│   ├── __init__.py
│   ├── config.py            # Config loading and validation
│   ├── aiven.py             # Aiven API client
│   ├── connectors.py        # Connector configuration builders
│   └── testing.py           # Functionality test suite
└── README.md
```
