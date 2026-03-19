# MySQL2PG

Automated MySQL-to-PostgreSQL migration powered by Kafka CDC (Change Data Capture). Runs on a DigitalOcean Droplet with self-signed HTTPS on port 8443.

Everything is configured through the web UI -- the only file you edit is `config.yaml` to set your Aiven API token and project name. The guided migration wizard handles discovery, compatibility analysis, schema creation, connector deployment, and live monitoring.

## Architecture

```
                          MySQL2PG Application (Droplet, HTTPS :8443)
                         ┌─────────────────────────────────────────┐
                         │  Discovery ─> Compatibility ─> Schema   │
                         │  Sizing ─> Deploy ─> Monitor ─> Verify  │
                         └────────────┬───────────────┬────────────┘
                                      │               │
              ┌───────────────────────┘               └──────────────────┐
              ▼                                                          ▼
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────────┐
│  MySQL Cluster   │────>│ Debezium Source   │────>│   Kafka Cluster      │
│  (DO Managed)    │     │ Connector         │     │   (DO Managed)       │
│                  │     └──────────────────┘     │                      │
│  - database_1    │                               │  topic per table     │
│  - database_2    │                               │  schema-history      │
│  - database_N    │                               └──────────┬───────────┘
└──────────────────┘                                          │
                                                              ▼
                         ┌──────────────────┐     ┌──────────────────────┐
                         │ JDBC Sink        │────>│  PostgreSQL Cluster  │
                         │ Connector        │     │  (DO Managed)        │
                         │ (per database)   │     │                      │
                         └──────────────────┘     │  - database_1        │
                                                  │  - database_2        │
                                                  │  - database_N        │
                                                  └──────────────────────┘
```

## Quick Install

One command installs everything on a fresh Ubuntu/Debian droplet:

```bash
curl -fsSL https://raw.githubusercontent.com/pagombin/SQL2PG/main/install.sh | bash
```

This installs Python, dependencies, generates self-signed TLS certificates, and starts a systemd service on HTTPS port 8443.

After installation:

1. Edit the configuration file with your Aiven credentials:

```bash
nano /opt/mysql2pg/config.yaml
```

You only need two values:

```yaml
aiven:
  token: "your-aiven-api-token"
  project: "do-user-xxxxxxx-0"
```

2. Restart the service:

```bash
systemctl restart mysql2pg
```

3. Open `https://<droplet-ip>:8443` in your browser
4. Click **Migration Wizard** to start a guided migration

### Updating

Re-run the same install command:

```bash
curl -fsSL https://raw.githubusercontent.com/pagombin/SQL2PG/main/install.sh | bash
```

The installer is fully idempotent. It updates only the application code while preserving your `config.yaml`, TLS certificates, migration state, and Python environment. No data is lost.

| Variable | Default | Description |
|----------|---------|-------------|
| `MYSQL2PG_BRANCH` | `main` | Git branch to install from |
| `MYSQL2PG_DIR` | `/opt/mysql2pg` | Installation directory |

## Migration Wizard

The wizard at `/migrate` handles the full migration lifecycle in 10 steps. On page refresh, it resumes any in-progress migration at the correct step.

### Step 1: Kafka Service

Select which Kafka cluster to use for CDC replication. The wizard lists all Kafka services in your Aiven project. On selection, it automatically:

- Enables Kafka Connect if not already active
- Enables auto-create topics
- Validates connectivity

### Step 2: Source Discovery

Connect to your MySQL cluster. The application introspects `INFORMATION_SCHEMA` to discover all databases, tables, columns (with full type metadata), indexes, foreign keys, triggers, views, and routines.

### Step 3: Database Selection

Choose which databases to migrate. Tables without primary keys are flagged (Debezium CDC requires a key for upsert mode).

### Step 4: Target Discovery

Connect to your PostgreSQL cluster. The application discovers existing databases, extensions (PostGIS availability), and user permissions.

### Step 5: Database Mapping

Map each MySQL database to a PostgreSQL target database. Select an existing database or have the wizard create a new one.

### Step 6: Compatibility Analysis

Every column is run through the type mapping engine and classified as BLOCK, WARN, or INFO:

| MySQL | PostgreSQL | Notes |
|-------|-----------|-------|
| `TINYINT(1)` | `BOOLEAN` | Semantic conversion |
| `INT UNSIGNED` | `BIGINT` | Widened for unsigned range |
| `BIGINT UNSIGNED` | `NUMERIC(20,0)` | No native unsigned 64-bit in PG |
| `DECIMAL(p,s)` | `NUMERIC(p,s)` | |
| `VARCHAR(n)` | `VARCHAR(n)` | |
| `TEXT` / `LONGTEXT` | `TEXT` | |
| `BLOB` / `LONGBLOB` | `BYTEA` | |
| `ENUM(...)` | Custom `CREATE TYPE` | Auto-generated per column |
| `SET(...)` | `TEXT` | Stored as comma-separated string |
| `JSON` | `JSONB` | |
| `DATETIME` | `TIMESTAMP WITHOUT TIME ZONE` | |
| `TIMESTAMP` | `TIMESTAMP WITH TIME ZONE` | |
| `BIT(1)` | `BOOLEAN` | Debezium emits as boolean |
| `BIT(n)` | `BYTEA` | Debezium emits as byte array |
| `AUTO_INCREMENT` | `GENERATED ALWAYS AS IDENTITY` | |
| `ON UPDATE CURRENT_TIMESTAMP` | Trigger function | Auto-generated |
| Spatial types | PostGIS geometry | Blocked if PostGIS unavailable |

### Step 7: Kafka Sizing

Validates Kafka cluster capacity against the migration workload (topics, throughput, storage, connector tasks). Shows upgrade recommendations if undersized.

### Step 8: Schema Creation

Generates and executes PostgreSQL DDL in order: ENUM types, tables (topologically sorted), indexes, trigger functions. Foreign keys are deferred to after data loading.

After execution, the wizard **validates** that every expected table exists on the PostgreSQL target before allowing connector deployment.

### Step 9: Deploy Connectors

Deploys a Debezium MySQL source connector and one JDBC sink connector per database. Source connectors include:

- `decimal.handling.mode=string` for safe DECIMAL round-tripping
- `time.precision.mode=connect` for standard time types
- `binary.handling.mode=base64` for JSON-safe binary data

Sink connectors include dead letter queues, error tolerance, and `stringtype=unspecified` for PostgreSQL ENUM compatibility.

### Step 10: Monitor

Real-time monitoring dashboard:

- **Row count comparison** -- source vs target for every table with delta and sync percentage
- **Sample data comparison** -- side-by-side view of rows from source and target
- **Connector status** -- live state of all connectors with per-connector Pause, Resume, Restart, and Delete actions
- **Failure traces** -- full Java stack traces displayed inline when connector tasks fail
- **Schema drift detection** -- re-discovers MySQL schema and compares against PostgreSQL to find new tables or column changes since migration started
- **Auto-refresh** -- configurable 10-second polling
- **FK constraint application** -- apply deferred foreign keys after initial data load
- **Delete all connectors** -- full teardown from the wizard

## Operations Dashboard

The main dashboard at `/` provides an operational overview independent of any specific migration:

- **Kafka service selector** -- dropdown in the header to choose which Kafka cluster to operate on
- **Setup** -- enable Kafka Connect and auto-create topics on the selected service
- **Verify** -- check Kafka cluster readiness
- **Teardown** -- delete all connectors on the selected service
- **Connectors tab** -- live status with per-connector Pause, Resume, Restart, Delete, and inline failure traces
- **Restart Failed** -- one-click restart of all failed connector tasks
- **Migrations tab** -- list all migrations with phase, source, databases, and timestamps

## Prerequisites

- Python 3.10+
- DigitalOcean account with Managed MySQL, PostgreSQL, and Kafka clusters
- Aiven API token (from the Aiven console or Atlantis)

## Configuration

`config.yaml` only requires Aiven API credentials. Everything else is configured through the web UI.

```yaml
aiven:
  token: "your-aiven-api-token"
  project: "do-user-xxxxxxx-0"
```

Values support `${ENV_VAR}` substitution:

```yaml
aiven:
  token: "${AIVEN_TOKEN}"
  project: "${AIVEN_PROJECT}"
```

For advanced CLI usage, optional sections can be added for `kafka`, `mysql_sources`, and `postgresql_target`. See `config.yaml.example` for details.

## REST API

### Dashboard API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/health` | GET | Health check |
| `/api/info` | GET | Configuration summary |
| `/api/status` | GET | Connector status (accepts `?service=`) |
| `/api/kafka-services` | GET | List Kafka services in Aiven project |
| `/api/setup` | POST | Enable Kafka Connect on a service |
| `/api/teardown` | POST | Delete connectors |
| `/api/verify` | GET | Verify Kafka readiness |
| `/api/restart-failed` | POST | Restart all failed connector tasks |
| `/api/connector/<name>` | DELETE | Delete a single connector |
| `/api/connector/<name>/pause` | POST | Pause a connector |
| `/api/connector/<name>/resume` | POST | Resume a connector |
| `/api/connector/<name>/restart` | POST | Restart a connector |

### Migration API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/migrations` | GET | List all migrations |
| `/api/migrations` | POST | Create a new migration |
| `/api/migrations/<id>` | GET | Get migration state |
| `/api/migrations/<id>` | DELETE | Delete a migration |
| `/api/migrations/<id>/select-kafka` | POST | Select Kafka service |
| `/api/migrations/<id>/discover-source` | POST | Discover MySQL schema |
| `/api/migrations/<id>/select-databases` | POST | Select databases |
| `/api/migrations/<id>/discover-target` | POST | Discover PostgreSQL target |
| `/api/migrations/<id>/database-mappings` | POST | Set DB mappings |
| `/api/migrations/<id>/analyze` | POST | Run compatibility analysis |
| `/api/migrations/<id>/validate-kafka` | POST | Validate Kafka capacity |
| `/api/migrations/<id>/create-schema` | POST | Generate and execute DDL |
| `/api/migrations/<id>/deploy-connectors` | POST | Deploy CDC connectors |
| `/api/migrations/<id>/apply-constraints` | POST | Apply deferred FK constraints |
| `/api/migrations/<id>/verify` | POST | Run verification checks |
| `/api/migrations/<id>/compare` | POST | Live data comparison |
| `/api/migrations/<id>/schema-drift` | POST | Detect schema changes |
| `/api/migrations/<id>/start-streaming` | POST | Enter streaming CDC mode |
| `/api/migrations/<id>/complete` | POST | Mark migration complete |

## CLI

The CLI is available for scripting and automation (requires full `config.yaml` with `kafka` and `mysql_sources` sections):

```bash
python3 main.py --version        # Show version
python3 main.py setup            # Enable Kafka Connect
python3 main.py plan             # Preview connector plan
python3 main.py deploy           # Deploy connectors
python3 main.py status           # Check connector status
python3 main.py verify           # Verify Kafka setup
python3 main.py teardown -y      # Delete all connectors
python3 main.py serve            # Start HTTPS dashboard
python3 main.py serve --workers 4  # With 4 gunicorn workers
python3 main.py serve --debug    # Single-threaded debug mode
```

## Service Management

```bash
systemctl start mysql2pg
systemctl stop mysql2pg
systemctl restart mysql2pg
systemctl status mysql2pg
journalctl -u mysql2pg -f
```

## Security Notes

- The web dashboard uses **self-signed TLS certificates**. Browsers will show a security warning on first visit.
- `config.yaml` contains your Aiven API token and is set to permissions `600`.
- MySQL and PostgreSQL passwords entered in the wizard are held in memory only and **never written to disk**.
- The service runs under a dedicated `mysql2pg` system user with no login shell.
