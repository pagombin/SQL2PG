"""Self-signed TLS certificate generation for HTTPS serving."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

DEFAULT_CERT_DIR = "/opt/mysql2pg/certs"
CERT_FILE = "server.crt"
KEY_FILE = "server.key"

CERT_SUBJECT = "/C=US/ST=State/L=City/O=MySQL2PG/OU=Migration/CN=mysql2pg"
CERT_DAYS = 3650


def get_cert_paths(cert_dir: str | Path = DEFAULT_CERT_DIR) -> tuple[Path, Path]:
    cert_dir = Path(cert_dir)
    return cert_dir / CERT_FILE, cert_dir / KEY_FILE


def certs_exist(cert_dir: str | Path = DEFAULT_CERT_DIR) -> bool:
    cert_path, key_path = get_cert_paths(cert_dir)
    return cert_path.exists() and key_path.exists()


def generate_self_signed_cert(
    cert_dir: str | Path = DEFAULT_CERT_DIR,
    hostname: str | None = None,
) -> tuple[Path, Path]:
    """Generate a self-signed TLS certificate and private key using openssl."""
    cert_dir = Path(cert_dir)
    cert_dir.mkdir(parents=True, exist_ok=True)

    cert_path, key_path = get_cert_paths(cert_dir)

    subject = CERT_SUBJECT
    if hostname:
        subject = f"/C=US/ST=State/L=City/O=MySQL2PG/OU=Migration/CN={hostname}"

    san_entries = ["DNS:localhost", "IP:127.0.0.1"]
    if hostname:
        san_entries.insert(0, f"DNS:{hostname}")

    san_config = ",".join(san_entries)

    subprocess.run(
        [
            "openssl", "req",
            "-x509",
            "-newkey", "rsa:4096",
            "-keyout", str(key_path),
            "-out", str(cert_path),
            "-days", str(CERT_DAYS),
            "-nodes",
            "-subj", subject,
            "-addext", f"subjectAltName={san_config}",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    os.chmod(str(key_path), 0o600)
    os.chmod(str(cert_path), 0o644)

    return cert_path, key_path
