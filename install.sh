#!/usr/bin/env bash
# MySQL2PG Installer / Updater
#
# Usage (fresh install or update):
#   curl -fsSL https://raw.githubusercontent.com/pagombin/SQL2PG/main/install.sh | bash
#
# Or download and run manually:
#   chmod +x install.sh && ./install.sh
#
# Environment variables (optional):
#   MYSQL2PG_BRANCH   - Git branch to install from (default: main)
#   MYSQL2PG_DIR      - Installation directory (default: /opt/mysql2pg)

set -euo pipefail

REPO="pagombin/SQL2PG"
BRANCH="${MYSQL2PG_BRANCH:-main}"
INSTALL_DIR="${MYSQL2PG_DIR:-/opt/mysql2pg}"
CERT_DIR="${INSTALL_DIR}/certs"
SERVICE_NAME="mysql2pg"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
RAW_BASE="https://raw.githubusercontent.com/${REPO}/${BRANCH}"
VENV_DIR="${INSTALL_DIR}/venv"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

log()  { echo -e "${GREEN}[mysql2pg]${NC} $*"; }
warn() { echo -e "${YELLOW}[mysql2pg]${NC} $*"; }
err()  { echo -e "${RED}[mysql2pg]${NC} $*" >&2; }
banner() { echo -e "\n${BOLD}${CYAN}$*${NC}\n"; }

check_root() {
    if [[ $EUID -ne 0 ]]; then
        err "This script must be run as root (or with sudo)."
        exit 1
    fi
}

detect_os() {
    if [[ -f /etc/os-release ]]; then
        . /etc/os-release
        OS_ID="${ID}"
        OS_VERSION="${VERSION_ID:-unknown}"
    else
        OS_ID="unknown"
        OS_VERSION="unknown"
    fi
    log "Detected OS: ${OS_ID} ${OS_VERSION}"
}

install_system_deps() {
    banner "Installing system dependencies"

    case "${OS_ID}" in
        ubuntu|debian)
            export DEBIAN_FRONTEND=noninteractive
            apt-get update -qq
            apt-get install -y -qq python3 python3-venv python3-pip openssl curl git > /dev/null
            ;;
        centos|rhel|rocky|almalinux|fedora)
            dnf install -y python3 python3-pip openssl curl git > /dev/null 2>&1 || \
            yum install -y python3 python3-pip openssl curl git > /dev/null 2>&1
            ;;
        *)
            warn "Unsupported OS '${OS_ID}'. Attempting to proceed anyway."
            ;;
    esac

    PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
    log "Python version: ${PYTHON_VERSION}"
}

create_user() {
    if ! id -u "${SERVICE_NAME}" &>/dev/null; then
        log "Creating system user: ${SERVICE_NAME}"
        useradd --system --no-create-home --shell /usr/sbin/nologin "${SERVICE_NAME}" || true
    fi
}

download_files() {
    banner "Downloading MySQL2PG"

    mkdir -p "${INSTALL_DIR}"

    local FILES=(
        "main.py"
        "requirements.txt"
        "config.yaml.example"
        "mysql2pg/__init__.py"
        "mysql2pg/config.py"
        "mysql2pg/aiven.py"
        "mysql2pg/connectors.py"
        "mysql2pg/testing.py"
        "mysql2pg/web.py"
        "mysql2pg/certs.py"
        "mysql2pg/templates/index.html"
        "mysql2pg.service"
    )

    mkdir -p "${INSTALL_DIR}/mysql2pg/templates"

    local UPDATED=0
    for file in "${FILES[@]}"; do
        local dest="${INSTALL_DIR}/${file}"
        local url="${RAW_BASE}/${file}"
        local tmp="${dest}.tmp"

        if curl -fsSL "${url}" -o "${tmp}" 2>/dev/null; then
            if [[ -f "${dest}" ]]; then
                if ! cmp -s "${tmp}" "${dest}"; then
                    mv "${tmp}" "${dest}"
                    log "  Updated: ${file}"
                    UPDATED=$((UPDATED + 1))
                else
                    rm -f "${tmp}"
                fi
            else
                mv "${tmp}" "${dest}"
                log "  Downloaded: ${file}"
                UPDATED=$((UPDATED + 1))
            fi
        else
            rm -f "${tmp}"
            warn "  Failed to download: ${file}"
        fi
    done

    if [[ ${UPDATED} -eq 0 ]]; then
        log "  All files are up to date."
    else
        log "  ${UPDATED} file(s) updated."
    fi

    echo "${UPDATED}"
}

setup_venv() {
    banner "Setting up Python virtual environment"

    if [[ ! -d "${VENV_DIR}" ]]; then
        log "Creating virtual environment..."
        python3 -m venv "${VENV_DIR}"
    fi

    log "Installing/updating dependencies..."
    "${VENV_DIR}/bin/pip" install --quiet --upgrade pip
    "${VENV_DIR}/bin/pip" install --quiet -r "${INSTALL_DIR}/requirements.txt"
    log "Dependencies installed."
}

generate_certs() {
    banner "TLS Certificates"

    if [[ -f "${CERT_DIR}/server.crt" && -f "${CERT_DIR}/server.key" ]]; then
        log "Certificates already exist in ${CERT_DIR}"
        return
    fi

    mkdir -p "${CERT_DIR}"

    local HOSTNAME
    HOSTNAME=$(hostname -f 2>/dev/null || hostname)

    log "Generating self-signed certificate for ${HOSTNAME}..."

    openssl req -x509 \
        -newkey rsa:4096 \
        -keyout "${CERT_DIR}/server.key" \
        -out "${CERT_DIR}/server.crt" \
        -days 3650 \
        -nodes \
        -subj "/C=US/ST=State/L=City/O=MySQL2PG/OU=Migration/CN=${HOSTNAME}" \
        -addext "subjectAltName=DNS:${HOSTNAME},DNS:localhost,IP:127.0.0.1" \
        2>/dev/null

    chmod 600 "${CERT_DIR}/server.key"
    chmod 644 "${CERT_DIR}/server.crt"
    log "Certificate generated."
}

setup_config() {
    if [[ ! -f "${INSTALL_DIR}/config.yaml" ]]; then
        if [[ -f "${INSTALL_DIR}/config.yaml.example" ]]; then
            cp "${INSTALL_DIR}/config.yaml.example" "${INSTALL_DIR}/config.yaml"
            warn "Created config.yaml from example. Edit it with your actual values:"
            warn "  nano ${INSTALL_DIR}/config.yaml"
        fi
    else
        log "config.yaml already exists, keeping current configuration."
    fi
}

set_permissions() {
    chown -R "${SERVICE_NAME}:${SERVICE_NAME}" "${INSTALL_DIR}"
    chmod 600 "${INSTALL_DIR}/config.yaml" 2>/dev/null || true
}

install_service() {
    banner "Configuring systemd service"

    cp "${INSTALL_DIR}/mysql2pg.service" "${SERVICE_FILE}"
    systemctl daemon-reload
    systemctl enable "${SERVICE_NAME}" 2>/dev/null

    log "Service installed and enabled."
}

restart_service() {
    if systemctl is-active --quiet "${SERVICE_NAME}"; then
        log "Restarting ${SERVICE_NAME} service..."
        systemctl restart "${SERVICE_NAME}"
    else
        log "Starting ${SERVICE_NAME} service..."
        systemctl start "${SERVICE_NAME}"
    fi

    sleep 2

    if systemctl is-active --quiet "${SERVICE_NAME}"; then
        log "Service is running."
    else
        warn "Service may not have started. Check logs with:"
        warn "  journalctl -u ${SERVICE_NAME} -f"
    fi
}

print_summary() {
    local IP
    IP=$(hostname -I 2>/dev/null | awk '{print $1}')
    [[ -z "${IP}" ]] && IP="<your-droplet-ip>"

    banner "Installation Complete"
    echo -e "  ${BOLD}Dashboard:${NC}    https://${IP}:8443"
    echo -e "  ${BOLD}API Health:${NC}   https://${IP}:8443/api/health"
    echo -e "  ${BOLD}Config:${NC}       ${INSTALL_DIR}/config.yaml"
    echo -e "  ${BOLD}Certs:${NC}        ${CERT_DIR}/"
    echo -e "  ${BOLD}Logs:${NC}         journalctl -u ${SERVICE_NAME} -f"
    echo -e "  ${BOLD}Service:${NC}      systemctl {start|stop|restart|status} ${SERVICE_NAME}"
    echo ""
    echo -e "  ${YELLOW}Note:${NC} The certificate is self-signed. Your browser will show a"
    echo -e "  security warning — this is expected. Accept it to proceed."
    echo ""
    echo -e "  ${BOLD}To update later:${NC}"
    echo -e "  curl -fsSL ${RAW_BASE}/install.sh | bash"
    echo ""
}

# ── Main ──────────────────────────────────────────────────────────────

main() {
    banner "MySQL2PG Installer"

    check_root
    detect_os
    install_system_deps
    create_user
    download_files
    setup_venv
    generate_certs
    setup_config
    set_permissions
    install_service
    restart_service
    print_summary
}

main "$@"
