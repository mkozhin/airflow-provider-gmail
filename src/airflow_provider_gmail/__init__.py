"""Apache Airflow provider for Gmail attachments.

Finds Gmail messages by criteria, selects the required attachments and stores
their bytes in S3-compatible storage or on local disk. The provider knows about
Gmail and about where to put the bytes; it does not parse file contents.
"""

from __future__ import annotations

try:
    from airflow_provider_gmail._version import __version__
except ImportError:  # pragma: no cover - only when setuptools-scm has not run
    __version__ = "0.0.0"


def get_provider_info() -> dict:
    """Return provider metadata for Airflow's ``ProvidersManager``.

    ``versions`` is derived from the setuptools-scm generated ``__version__`` so
    it never drifts away from the wheel/tag version.

    ``connection-types`` is intentionally absent here: it is added in Task 3
    together with ``GmailHook``. Adding a ``hook-class-name`` that points at a
    class that does not yet exist makes ``ProvidersManager`` fail to load.
    """
    return {
        "package-name": "airflow-provider-gmail",
        "name": "Gmail",
        "description": (
            "Finds Gmail messages by criteria and stores their attachments in "
            "S3-compatible storage or on local disk."
        ),
        "versions": [__version__],
    }
