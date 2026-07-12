"""Tests for provider registration via ``get_provider_info()``."""

from __future__ import annotations

import airflow_provider_gmail
from airflow_provider_gmail import get_provider_info


def test_provider_info_has_required_keys():
    info = get_provider_info()
    for key in ("package-name", "name", "description", "versions"):
        assert key in info, f"missing required key: {key}"
    assert info["package-name"] == "airflow-provider-gmail"
    assert info["name"] == "Gmail"
    assert isinstance(info["description"], str) and info["description"]


def test_versions_match_package_version():
    info = get_provider_info()
    assert info["versions"] == [airflow_provider_gmail.__version__]


def test_connection_types_absent_until_hook_exists():
    # GmailHook does not exist yet (Task 3). Registering connection-types now
    # would make ProvidersManager fail to load the provider.
    assert "connection-types" not in get_provider_info()
