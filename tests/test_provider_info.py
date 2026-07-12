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


def test_connection_types_registers_gmail_hook():
    # Task 3: GmailHook exists, so connection-types is now present and points at it.
    info = get_provider_info()
    conn_types = info["connection-types"]
    assert conn_types == [
        {
            "connection-type": "gmail",
            "hook-class-name": "airflow_provider_gmail.hooks.gmail.GmailHook",
        }
    ]


def test_providers_manager_loads_gmail_hook_class():
    # Verify ProvidersManager actually imports the hook class, not just the
    # string key: a broken hook-class-name would leave the class field empty.
    from airflow.providers_manager import ProvidersManager

    hooks = ProvidersManager().hooks
    assert "gmail" in hooks, f"gmail not registered; have {sorted(hooks)}"
    info = hooks["gmail"]
    # A non-None HookInfo means ProvidersManager imported the class successfully
    # (a broken hook-class-name logs a warning and leaves this None). hook_name
    # is read off the imported class, proving the real class loaded.
    assert info is not None, "gmail hook failed to import in ProvidersManager"
    assert info.connection_type == "gmail"
    assert info.hook_class_name == "airflow_provider_gmail.hooks.gmail.GmailHook"
    assert info.hook_name == "Gmail"
