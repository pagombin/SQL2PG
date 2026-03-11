# MySQL2PG

A full-featured MySQL-to-PostgreSQL migration application powered by Kafka CDC (Change Data Capture). Runs on a DigitalOcean Droplet with self-signed HTTPS on port 8443.

The application provides a guided migration wizard that automatically **discovers** your MySQL schemas (databases, tables, columns, indexes, foreign keys, triggers, views, routines), **analyzes** type compatibility, **generates** PostgreSQL DDL, **validates** Kafka cluster capacity, **deploys** Debezium CDC connectors, and provides **live data comparison** between source and target throughout the migration.

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

## Quick Install (DigitalOcean Droplet)

One command installs everything on a fresh Ubuntu/Debian droplet:

```bash
curl -fsSL https://raw.githubusercontent.com/pagombin/SQL2PG/main/install.sh | bash
```

This installs Python, dependencies, generates self-signed TLS certificates, and starts a systemd service on HTTPS port 8443.

After installation:

1. Edit the configuration file with your Aiven/Kafka credentials:

```bash
nano /opt/mysql2pg/config.yaml
```

2. Restart the service:

```bash
systemctl restart mysql2pg
```

3. Open `https://<droplet-ip>:8443` in your browser
4. Click **Migration Wizard** to start a guided migration

### Updating

Re-run the same command to check for updates:

```bash
curl -fsSL https://raw.githubusercontent.com/pagombin/SQL2PG/main/install.sh | bash
```

The script downloads only changed files, updates dependencies if needed, preserves your existing `config.yaml` and TLS certificates, and restarts the service.

| Variable | Default | Description |
|----------|---------|-------------|
| `MYSQL2PG_BRANCH` | `main` | Git branch to install from |
| `MYSQL2PG_DIR` | `/opt/mysql2pg` | Installation directory |

## Migration Wizard

The core of the application is a 9-step guided migration wizard accessible at `/migrate`. It handles the full lifecycle from discovery to ongoing CDC streaming.

### Step 1: Source Discovery

Connect to your MySQL cluster with credentials. The application introspects `INFORMATION_SCHEMA` to discover:

- All user databases (system databases are filtered out)
- Tables with row counts, data sizes, engine types, auto-increment values
- Columns with full type information (data type, UNSIGNED, ZEROFILL, character set, collation, defaults, ON UPDATE, generation expressions)
- Indexes (primary keys, unique, BTREE, FULLTEXT, SPATIAL)
- Foreign key relationships (columns, referenced tables, ON DELETE/UPDATE rules)
- Triggers, views, and stored routines
- MySQL server version and SQL mode

### Step 2: Database Selection

Choose which databases to migrate. The wizard shows each database with table count, total rows, total size, and flags tables without primary keys (which require special handling for CDC).

### Step 3: Target Discovery

Connect to your PostgreSQL cluster. The application discovers:

- Existing databases with owners, encodings, and sizes
- Available extensions (PostGIS availability is checked for spatial type support)
- User permissions (can the user create databases?)

### Step 4: Database Mapping

Map each selected MySQL database to a PostgreSQL target database. You can select an existing database or have the application create a new one.

### Step 5: Compatibility Analysis

Every column across all selected tables is run through the type mapping engine. The report classifies each mapping as:

- **BLOCK** -- Migration cannot proceed. The column type has no PostgreSQL equivalent (e.g., virtual generated columns, spatial types without PostGIS).
- **WARN** -- Conversion is possible but involves behavioral changes (e.g., INT UNSIGNED widened to BIGINT, SET mapped to TEXT array, YEAR mapped to SMALLINT).
- **INFO** -- Direct automatic mapping, no action needed.

#### Type Mapping Summary

| MySQL | PostgreSQL | Notes |
|-------|-----------|-------|
| `TINYINT(1)` | `BOOLEAN` | Semantic conversion |
| `TINYINT` | `SMALLINT` | |
| `INT` | `INTEGER` | |
| `INT UNSIGNED` | `BIGINT` | Widened to fit unsigned range |
| `BIGINT UNSIGNED` | `NUMERIC(20,0)` | No native unsigned 64-bit in PG |
| `DECIMAL(p,s)` | `NUMERIC(p,s)` | |
| `FLOAT` / `DOUBLE` | `REAL` / `DOUBLE PRECISION` | |
| `VARCHAR(n)` | `VARCHAR(n)` | |
| `TEXT` / `LONGTEXT` | `TEXT` | |
| `BLOB` / `LONGBLOB` | `BYTEA` | |
| `ENUM(...)` | Custom `CREATE TYPE` | Auto-generated per column |
| `SET(...)` | `TEXT[]` | Behavioral difference |
| `JSON` | `JSONB` | Indexed by default |
| `DATETIME` | `TIMESTAMP WITHOUT TIME ZONE` | |
| `TIMESTAMP` | `TIMESTAMP WITH TIME ZONE` | |
| `AUTO_INCREMENT` | `GENERATED ALWAYS AS IDENTITY` | |
| `ON UPDATE CURRENT_TIMESTAMP` | Trigger function | Auto-generated |
| Spatial types | PostGIS geometry | Blocked if PostGIS unavailable |
| `GENERATED ... VIRTUAL` | -- | Blocked (PG only supports STORED) |

Additional checks:
- Tables without primary keys are flagged (Debezium CDC requires a key for upsert mode)
- Cross-database foreign keys are blocked (PostgreSQL databases are isolated)
- Stored routines are flagged as requiring manual PL/pgSQL conversion
- Views are noted as not migrated by CDC

### Step 6: Kafka Sizing Validation

The application queries the Aiven API for your Kafka cluster's resources and calculates:

- Topics needed (one per table plus schema history)
- Required throughput based on total data size
- Estimated migration duration
- Storage requirements with replication factor
- Connector task capacity

If the cluster is undersized, specific upgrade recommendations are shown.

### Step 7: Schema Creation

PostgreSQL DDL is generated and executed:

1. **ENUM types** -- `CREATE TYPE ... AS ENUM` for all MySQL ENUM columns
2. **Tables** -- Created in topological order (parents before children based on FK graph). Circular FK dependencies are detected and handled.
3. **Indexes** -- All non-PK indexes are recreated (prefix indexes converted to expression indexes)
4. **Trigger functions** -- Auto-generated for `ON UPDATE CURRENT_TIMESTAMP` behavior
5. **Foreign keys** -- Deferred to after data loading to avoid constraint violations during CDC snapshot

### Step 8: Connector Deployment

The application deploys:

- **One Debezium MySQL source connector** covering all selected databases with `message.key.columns` set for PK-less tables
- **One JDBC sink connector per database** with discovery-driven `pk.fields`, explicit topic lists, `auto.evolve` for schema changes, and dead letter queue configuration

### Step 9: Monitor and Live Data Comparison

Real-time monitoring dashboard showing:

- **Row count comparison** -- Source vs target for every table, with delta and sync percentage
- **Per-database summaries** -- Aggregated counts and sync status
- **Sample data comparison** -- Side-by-side view of first N rows from source and target (ordered by PK)
- **Auto-refresh** -- Configurable 10-second polling
- **FK constraint application** -- Apply deferred foreign key constraints after initial data load is complete
- **Migration completion** -- Mark migration as done when fully synced

## Operations Dashboard

The main dashboard at `/` provides quick actions for the underlying connector infrastructure:

- **Setup** -- Enable Kafka Connect and auto-create topics
- **Deploy** -- Deploy connectors from config.yaml
- **Verify** -- Check Kafka cluster readiness
- **Teardown** -- Delete all connectors
- **Connectors tab** -- Live status with per-connector pause/resume/restart
- **Plan tab** -- Preview connector deployment plan
- **Configuration tab** -- View current config summary

## Prerequisites

- Python 3.10+
- DigitalOcean account with Managed MySQL, PostgreSQL, and Kafka clusters
- Aiven API token (from Atlantis or Aiven console)
- Kafka cluster SSL connection details (hostname and port from the **SSL** tab, not SASL)

## Configuration

The `config.yaml` file provides Aiven/Kafka credentials used by both the dashboard and migration wizard. MySQL and PostgreSQL credentials for migrations are entered at runtime through the wizard UI (not stored in config).

```bash
cp config.yaml.example config.yaml
```

| Section | Field | Description |
|---------|-------|-------------|
| `aiven.token` | | Aiven API bearer token |
| `aiven.project` | | Aiven project name (e.g., `do-user-xxxxxxx-0`) |
| `kafka.service_name` | | Kafka cluster name in DigitalOcean |
| `kafka.ssl_host` | | Kafka SSL hostname (Connection Details > SSL tab) |
| `kafka.ssl_port` | | Kafka SSL port |
| `mysql_sources[]` | | MySQL sources for the config-driven dashboard (not required for wizard) |
| `postgresql_target` | | PostgreSQL target for the config-driven dashboard (not required for wizard) |
| `table_name_strategy` | | `as_is` or `prefixed` (config-driven mode only) |

Config values support `${ENV_VAR}` substitution:

```yaml
aiven:
  token: "${AIVEN_TOKEN}"
```

## REST API

### Dashboard API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/health` | GET | Health check |
| `/api/info` | GET | Configuration summary |
| `/api/plan` | GET | Connector deployment plan |
| `/api/status` | GET | Deployed connector status |
| `/api/setup` | POST | Enable Kafka Connect |
| `/api/deploy` | POST | Deploy connectors |
| `/api/teardown` | POST | Delete connectors |
| `/api/verify` | GET | Verify Kafka readiness |
| `/api/connector/<name>/pause` | POST | Pause a connector |
| `/api/connector/<name>/resume` | POST | Resume a connector |
| `/api/connector/<name>/restart` | POST | Restart a connector |
| `/api/config/reload` | POST | Reload config from disk |

### Migration API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/migrations` | GET | List all migrations |
| `/api/migrations` | POST | Create a new migration |
| `/api/migrations/<id>` | GET | Get migration state |
| `/api/migrations/<id>` | DELETE | Delete a migration |
| `/api/migrations/<id>/discover-source` | POST | Discover MySQL schema |
| `/api/migrations/<id>/select-databases` | POST | Select databases to migrate |
| `/api/migrations/<id>/discover-target` | POST | Discover PostgreSQL target |
| `/api/migrations/<id>/database-mappings` | POST | Set source-to-target DB mappings |
| `/api/migrations/<id>/analyze` | POST | Run compatibility analysis |
| `/api/migrations/<id>/validate-kafka` | POST | Validate Kafka capacity |
| `/api/migrations/<id>/create-schema` | POST | Generate and execute DDL |
| `/api/migrations/<id>/deploy-connectors` | POST | Deploy CDC connectors |
| `/api/migrations/<id>/apply-constraints` | POST | Apply deferred FK constraints |
| `/api/migrations/<id>/verify` | POST | Run verification checks |
| `/api/migrations/<id>/compare` | POST | Live data comparison |
| `/api/migrations/<id>/start-streaming` | POST | Enter streaming CDC mode |
| `/api/migrations/<id>/complete` | POST | Mark migration complete |

## CLI Usage

The CLI remains fully functional for scripting and automation:

```bash
python3 main.py setup       # Enable Kafka Connect & auto-create topics
python3 main.py plan        # Preview connector deployment plan
python3 main.py deploy      # Deploy all connectors
python3 main.py status      # Check connector status
python3 main.py list        # List all connectors
python3 main.py verify      # Verify Kafka cluster setup
python3 main.py test        # Run end-to-end CDC test
python3 main.py teardown -y # Delete all connectors
python3 main.py info        # Display configuration summary
python3 main.py serve       # Start HTTPS web dashboard
python3 main.py run         # Full pipeline: setup -> deploy -> verify -> test
```

## Service Management

```bash
systemctl start mysql2pg
systemctl stop mysql2pg
systemctl restart mysql2pg
systemctl status mysql2pg
journalctl -u mysql2pg -f
```

## Project Structure

```
├── main.py                          # CLI entry point (includes 'serve' command)
├── install.sh                       # One-line installer/updater for droplets
├── mysql2pg.service                 # Systemd unit file
├── config.yaml.example             # Configuration template
├── requirements.txt                 # Python dependencies
├── mysql2pg/
│   ├── __init__.py
│   ├── models.py                    # Data models (schema, migration state, comparisons)
│   ├── discovery.py                 # MySQL + PostgreSQL schema introspection
│   ├── compatibility.py            # Type mapping engine and compatibility analysis
│   ├── schema.py                    # PostgreSQL DDL generation
│   ├── sizing.py                    # Kafka cluster capacity validation
│   ├── migration.py                 # Migration state machine orchestrator
│   ├── verification.py             # Post-migration verification and live comparison
│   ├── connectors.py               # Connector configuration builders (config + discovery)
│   ├── config.py                    # YAML config loading and validation
│   ├── aiven.py                     # Aiven API client
│   ├── testing.py                   # End-to-end CDC functionality test
│   ├── web.py                       # Flask application (dashboard + migration API)
│   ├── certs.py                     # Self-signed TLS certificate generation
│   └── templates/
│       ├── index.html               # Operations dashboard UI
│       └── migrate.html             # Migration wizard UI with live comparison
└── README.md
```

### Module Responsibilities

| Module | Purpose |
|--------|---------|
| `models.py` | 30+ dataclasses for schema representation, migration state (15 phases), compatibility reports, Kafka sizing, and live comparison results |
| `discovery.py` | Full MySQL `INFORMATION_SCHEMA` introspection (databases, tables, columns, indexes, FKs, triggers, views, routines). PostgreSQL target discovery. FK dependency graph with topological sort and cycle detection. |
| `compatibility.py` | Maps every MySQL column type to PostgreSQL with BLOCK/WARN/INFO severity. Handles UNSIGNED widening, ENUM->CREATE TYPE, SET->TEXT[], spatial->PostGIS, virtual generated columns. |
| `schema.py` | Generates PostgreSQL DDL: ENUM types, tables in FK-dependency order, indexes, trigger functions for ON UPDATE CURRENT_TIMESTAMP, deferred FK constraints. |
| `sizing.py` | Calculates topics, partitions, throughput, storage needs. Compares against Kafka cluster resources via Aiven API. |
| `migration.py` | State machine with 15+ phases from CREATED to COMPLETED. Persistent state (JSON on disk) survives restarts. Coordinates the full pipeline. |
| `verification.py` | Row count comparison, schema match verification, FK integrity checks, sample data comparison, full live comparison with per-database aggregation. |
| `connectors.py` | Builds Debezium source and JDBC sink connector configs. Discovery-driven mode sets `pk.fields`, `message.key.columns`, and DLQ per table. |

## Migration State Lifecycle

```
CREATED -> DISCOVERING_SOURCE -> SOURCE_DISCOVERED -> DATABASES_SELECTED
  -> DISCOVERING_TARGET -> TARGET_DISCOVERED -> ANALYZING_COMPATIBILITY
  -> COMPATIBILITY_REVIEWED -> VALIDATING_KAFKA -> KAFKA_VALIDATED
  -> CREATING_SCHEMA -> SCHEMA_CREATED -> DEPLOYING_CONNECTORS
  -> CONNECTORS_RUNNING -> SNAPSHOT_IN_PROGRESS -> SNAPSHOT_COMPLETE
  -> APPLYING_CONSTRAINTS -> CONSTRAINTS_APPLIED -> VERIFYING -> VERIFIED
  -> STREAMING -> COMPLETED
```

Migration state is persisted to `/opt/mysql2pg/migrations/<id>.json` and survives server restarts. Credentials are held in memory only and never written to disk.

## Security Notes

- The web dashboard uses **self-signed TLS certificates**. Browsers will show a security warning on first visit -- this is expected.
- `config.yaml` contains Aiven/Kafka credentials and is excluded from git via `.gitignore`. Permissions are set to `600`.
- MySQL and PostgreSQL passwords entered in the migration wizard are held in memory only and **never persisted to disk**.
- The service runs under a dedicated `mysql2pg` system user with no login shell.
- Consider using a firewall (e.g., `ufw allow 8443/tcp`) to restrict dashboard access.
