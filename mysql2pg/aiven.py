"""Aiven API client for Kafka Connect management."""

from __future__ import annotations

import time
from typing import Any, Optional

import requests

BASE_URL = "https://api.aiven.io/v1"

DEFAULT_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY = 5


class AivenAPIError(Exception):
    """Raised when an Aiven API call fails."""

    def __init__(self, message: str, status_code: int | None = None, response_body: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body


class AivenClient:
    """Client for interacting with the Aiven API."""

    def __init__(self, token: str, project: str):
        self.token = token
        self.project = project
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"bearer {token}",
            "Content-Type": "application/json",
        })

    def _url(self, path: str) -> str:
        return f"{BASE_URL}/project/{self.project}{path}"

    def _request(
        self,
        method: str,
        path: str,
        data: dict | None = None,
        retries: int = MAX_RETRIES,
    ) -> dict:
        url = self._url(path)
        last_error: Exception | None = None

        for attempt in range(1, retries + 1):
            try:
                resp = self.session.request(
                    method, url, json=data, timeout=DEFAULT_TIMEOUT
                )
                if resp.status_code >= 400:
                    body = resp.json() if resp.content else {}
                    msg = body.get("message", resp.text)
                    raise AivenAPIError(
                        f"API {method} {path} failed ({resp.status_code}): {msg}",
                        status_code=resp.status_code,
                        response_body=body,
                    )
                return resp.json() if resp.content else {}
            except requests.RequestException as e:
                last_error = e
                if attempt < retries:
                    time.sleep(RETRY_DELAY * attempt)
                    continue
                raise AivenAPIError(f"Request failed after {retries} attempts: {e}") from e

        raise AivenAPIError(f"Request failed: {last_error}")

    # ── Service Management ───────────────────────────────────────────

    def get_service(self, service_name: str) -> dict:
        """Get full service details."""
        return self._request("GET", f"/service/{service_name}")

    def get_service_features(self, service_name: str) -> dict:
        """Get service feature flags."""
        data = self.get_service(service_name)
        return data.get("service", {}).get("features", {})

    def update_service_config(self, service_name: str, user_config: dict) -> dict:
        """Update service user configuration."""
        return self._request("PUT", f"/service/{service_name}", {"user_config": user_config})

    def list_kafka_services(self) -> list[dict]:
        """List all Kafka services in the project with Connect eligibility."""
        data = self._request("GET", "/service")
        services = data.get("services", [])
        result = []
        for s in services:
            if s.get("service_type") != "kafka":
                continue
            features = s.get("features", {})
            kafka_connect_enabled = features.get("kafka_connect", False)

            # Determine if the plan supports Kafka Connect.  Plans that
            # include "hobbyist" or "startup" generally do not; business
            # and premium plans do.  We also inspect the feature flags
            # and the kafka_connect_service_integration feature.
            plan = s.get("plan", "")
            plan_lower = plan.lower()
            connect_supported = True
            if any(p in plan_lower for p in ("hobbyist", "startup")):
                connect_supported = False
            if features.get("kafka_connect_service_integration") is False and not kafka_connect_enabled:
                connect_supported = False

            result.append({
                "name": s["service_name"],
                "plan": plan,
                "state": s.get("state", ""),
                "cloud": s.get("cloud_name", ""),
                "kafka_connect_enabled": kafka_connect_enabled,
                "kafka_connect_supported": connect_supported,
            })
        return result

    # ── Kafka Connect ────────────────────────────────────────────────

    def enable_kafka_connect(self, service_name: str) -> dict:
        """Enable Kafka Connect on a Kafka service."""
        return self.update_service_config(service_name, {"kafka_connect": True})

    def verify_kafka_connect(self, service_name: str) -> bool:
        """Check if Kafka Connect is enabled on the service."""
        features = self.get_service_features(service_name)
        return features.get("kafka_connect", False)

    def enable_auto_create_topics(self, service_name: str) -> dict:
        """Enable auto.create.topics.enable on the Kafka cluster."""
        return self.update_service_config(
            service_name,
            {"kafka": {"auto_create_topics_enable": True}},
        )

    def get_kafka_ssl_endpoint(self, service_name: str) -> tuple[str, int]:
        """Resolve the Kafka broker SSL host and port from the Aiven API.

        Inspects the service's connection_info and components to find the
        SSL bootstrap endpoint.  Raises AivenAPIError if it cannot be
        determined.
        """
        data = self.get_service(service_name)
        service = data.get("service", {})

        # service_uri is typically "host:port" for the primary SSL endpoint
        service_uri = service.get("service_uri", "")
        if service_uri:
            # Format: host:port or kafka://host:port
            uri = service_uri.split("://")[-1]
            if ":" in uri:
                host, port_str = uri.rsplit(":", 1)
                try:
                    return host, int(port_str)
                except ValueError:
                    pass

        # Fall back to components list
        for component in service.get("components", []):
            if component.get("component") == "kafka" and component.get("route") == "dynamic":
                return component["host"], int(component["port"])

        # Try any kafka component
        for component in service.get("components", []):
            if component.get("component") == "kafka":
                return component["host"], int(component["port"])

        raise AivenAPIError(
            f"Could not determine Kafka SSL endpoint for service '{service_name}'. "
            f"Set ssl_host and ssl_port in config.yaml manually."
        )

    def get_kafka_connect_uri(self, service_name: str) -> str | None:
        """
        Retrieve the kafka_connect_uri from service connection info.
        Returns the full URI (e.g., https://user:pass@host:port).
        """
        data = self.get_service(service_name)
        service = data.get("service", {})
        connect_uri = service.get("connection_info", {}).get("kafka_connect_uri")
        if connect_uri:
            return connect_uri

        for component in service.get("components", []):
            if component.get("component") == "kafka_connect":
                host = component["host"]
                port = component["port"]
                route = component.get("route", "dynamic")
                return f"https://{host}:{port}"
        return None

    # ── Connector CRUD ───────────────────────────────────────────────

    def create_connector(self, service_name: str, connector_config: dict) -> dict:
        """Create a new Kafka connector."""
        return self._request(
            "POST", f"/service/{service_name}/connectors", connector_config
        )

    def list_connectors(self, service_name: str) -> dict:
        """List all connectors on a Kafka service."""
        return self._request("GET", f"/service/{service_name}/connectors")

    def get_connector(self, service_name: str, connector_name: str) -> dict:
        """Get details of a specific connector."""
        return self._request(
            "GET", f"/service/{service_name}/connectors/{connector_name}"
        )

    def delete_connector(self, service_name: str, connector_name: str) -> dict:
        """Delete a connector."""
        return self._request(
            "DELETE", f"/service/{service_name}/connectors/{connector_name}"
        )

    def get_connector_status(self, service_name: str, connector_name: str) -> dict:
        """Get the status of a connector."""
        return self._request(
            "GET", f"/service/{service_name}/connectors/{connector_name}/status"
        )

    def pause_connector(self, service_name: str, connector_name: str) -> dict:
        """Pause a connector."""
        return self._request(
            "POST", f"/service/{service_name}/connectors/{connector_name}/pause"
        )

    def resume_connector(self, service_name: str, connector_name: str) -> dict:
        """Resume a paused connector."""
        return self._request(
            "POST", f"/service/{service_name}/connectors/{connector_name}/resume"
        )

    def restart_connector(self, service_name: str, connector_name: str) -> dict:
        """Restart a connector."""
        return self._request(
            "POST", f"/service/{service_name}/connectors/{connector_name}/restart"
        )

    # ── Polling / Waiting ────────────────────────────────────────────

    def wait_for_kafka_connect(
        self, service_name: str, timeout: int = 300, poll_interval: int = 10
    ) -> bool:
        """Poll until Kafka Connect is enabled, or timeout."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.verify_kafka_connect(service_name):
                return True
            time.sleep(poll_interval)
        return False

    def wait_for_connector_running(
        self,
        service_name: str,
        connector_name: str,
        timeout: int = 300,
        poll_interval: int = 10,
    ) -> dict:
        """Poll until a connector reaches RUNNING state, or timeout."""
        deadline = time.time() + timeout
        last_status: dict = {}
        while time.time() < deadline:
            try:
                status = self.get_connector_status(service_name, connector_name)
                last_status = status
                state = status.get("status", {}).get("state", "UNKNOWN")
                if state == "RUNNING":
                    return status
                if state == "FAILED":
                    return status
            except AivenAPIError:
                pass
            time.sleep(poll_interval)
        return last_status
