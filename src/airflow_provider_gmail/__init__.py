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

    ``connection-types`` registers ``GmailHook`` so the Gmail connection type
    appears in the Airflow UI. The ``hook-class-name`` must point at a class that
    actually imports, or ``ProvidersManager`` fails to load the provider.
    """
    return {
        "package-name": "airflow-provider-gmail",
        "name": "Gmail",
        "description": (
            "Finds Gmail messages by criteria and stores their attachments in "
            "S3-compatible storage or on local disk."
        ),
        "versions": [__version__],
        "connection-types": [
            {
                "connection-type": "gmail",
                "hook-class-name": "airflow_provider_gmail.hooks.gmail.GmailHook",
            }
        ],
    }
