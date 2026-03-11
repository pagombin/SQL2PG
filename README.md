# MySQL2PG

Automated MySQL-to-PostgreSQL migration via Kafka CDC (Change Data Capture).

Supports **multiple MySQL source databases** replicating to a **single PostgreSQL target** through DigitalOcean Managed Kafka using Debezium CDC source connectors and JDBC sink connectors, orchestrated via the Aiven API.

Runs as a web dashboard on a DigitalOcean Droplet with self-signed HTTPS on port 8443.

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

## Quick Install (DigitalOcean Droplet)

One command installs everything on a fresh Ubuntu/Debian droplet — Python, dependencies, self-signed TLS certificates, and a systemd service serving on HTTPS port 8443:

```bash
curl -fsSL https://raw.githubusercontent.com/pagombin/SQL2PG/main/install.sh | bash
```

After installation:

1. Edit the configuration file:
   ```bash
   nano /opt/mysql2pg/config.yaml
   ```
2. Restart the service:
   ```bash
   systemctl restart mysql2pg
   ```
3. Open your browser at `https://<droplet-ip>:8443`

### Updating

Re-run the same install command to check for updates and apply them:

```bash
curl -fsSL https://raw.githubusercontent.com/pagombin/SQL2PG/main/install.sh | bash
```

The script will:
- Download only changed files
- Update Python dependencies if `requirements.txt` changed
- Preserve your existing `config.yaml` and TLS certificates
- Restart the service automatically

### Install Options

| Variable | Default | Description |
|----------|---------|-------------|
| `MYSQL2PG_BRANCH` | `main` | Git branch to install from |
| `MYSQL2PG_DIR` | `/opt/mysql2pg` | Installation directory |

Example using a different branch:

```bash
MYSQL2PG_BRANCH=develop curl -fsSL https://raw.githubusercontent.com/pagombin/SQL2PG/develop/install.sh | bash
```

## Prerequisites

- Python 3.10+
- DigitalOcean account with Managed MySQL, PostgreSQL, and Kafka clusters provisioned
- Aiven API token (retrievable from Atlantis or Aiven console)
- Kafka cluster SSL connection details (hostname and port from the **SSL** tab, not SASL)

## Manual Installation (Development)

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

## Web Dashboard

The web dashboard runs on HTTPS port 8443 with self-signed certificates. It provides:

- **Dashboard** — Quick actions (setup, deploy, verify, teardown), status cards, activity log
- **Connectors** — Live status of all deployed connectors with pause/resume/restart controls
- **Plan** — Preview connector deployment plan before deploying
- **Configuration** — View current configuration summary

### Starting the Web Server

Via systemd (production):

```bash
systemctl start mysql2pg
```

Manually (development):

```bash
python3 main.py serve
python3 main.py serve --port 9443 --debug
python3 main.py serve --cert-dir ./my-certs
```

### REST API

All dashboard actions are available via JSON API:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/health` | GET | Health check |
| `/api/info` | GET | Configuration summary |
| `/api/plan` | GET | Connector deployment plan |
| `/api/status` | GET | Status of all deployed connectors |
| `/api/setup` | POST | Enable Kafka Connect and auto-create topics |
| `/api/deploy` | POST | Deploy connectors (body: `{"type": "source"\|"sink"}`) |
| `/api/teardown` | POST | Delete connectors (body: `{"all": true}` for all) |
| `/api/verify` | GET | Verify Kafka cluster readiness |
| `/api/connector/<name>/pause` | POST | Pause a connector |
| `/api/connector/<name>/resume` | POST | Resume a connector |
| `/api/connector/<name>/restart` | POST | Restart a connector |
| `/api/config/reload` | POST | Reload configuration from disk |

## CLI Usage

The CLI is still fully functional for scripting and automation.

### Full Pipeline (Recommended)

```bash
python3 main.py run
```

Options:
- `--skip-setup` — skip Kafka Connect enablement (if already configured)
- `--skip-test` — skip the functionality test
- `--timeout N` — connector startup timeout in seconds (default: 300)

### Individual Commands

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
```

### Using a Custom Config File

```bash
python3 main.py -c production.yaml run
python3 main.py -c production.yaml serve
```

## Service Management

```bash
systemctl start mysql2pg      # Start the service
systemctl stop mysql2pg       # Stop the service
systemctl restart mysql2pg    # Restart after config changes
systemctl status mysql2pg     # Check service status
journalctl -u mysql2pg -f     # Follow live logs
```

## Multiple MySQL Sources

The tool handles multiple MySQL source instances, each potentially containing multiple databases. For each MySQL source:

1. A single **Debezium source connector** is created, covering all databases in that instance via `database.include.list`.
2. A **JDBC sink connector** is created per database, routing topics to the correct PostgreSQL tables.

### Topic Naming

Topics follow the pattern: `mysql_cdc_{source-name}.{database}.{table}`

### Table Name Collision

When replicating from multiple sources that have tables with the same name, use `table_name_strategy: prefixed`:

```yaml
table_name_strategy: "prefixed"
```

This produces PostgreSQL table names like `inventory_customers` instead of just `customers`.

## Project Structure

```
├── main.py                      # CLI entry point (includes 'serve' command)
├── install.sh                   # One-line installer/updater for droplets
├── mysql2pg.service             # Systemd unit file
├── config.yaml.example          # Configuration template
├── requirements.txt             # Python dependencies
├── mysql2pg/
│   ├── __init__.py
│   ├── config.py                # Config loading and validation
│   ├── aiven.py                 # Aiven API client
│   ├── connectors.py            # Connector configuration builders
│   ├── testing.py               # Functionality test suite
│   ├── web.py                   # Flask web application (HTTPS dashboard + API)
│   ├── certs.py                 # Self-signed TLS certificate generation
│   └── templates/
│       └── index.html           # Web dashboard UI
└── README.md
```

## Security Notes

- The web dashboard uses **self-signed TLS certificates**. Browsers will show a security warning on first visit — this is expected.
- `config.yaml` contains sensitive credentials and is excluded from git via `.gitignore`.
- The install script sets `config.yaml` permissions to `600` (owner-read/write only).
- The service runs under a dedicated `mysql2pg` system user with no login shell.
- Consider using a firewall (e.g., `ufw allow 8443/tcp`) to restrict access to the dashboard.
