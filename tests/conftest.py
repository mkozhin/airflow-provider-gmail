"""Shared test helpers.

Not a pytest fixture module in the ordinary sense: pytest auto-discovers
``conftest.py`` for fixtures/hooks only, so a plain function defined here is
**not** automatically available in other test modules — each module that
wants :func:`render_fields` must ``from conftest import render_fields``
explicitly. ``tests/`` carries no ``__init__.py``, so pytest inserts the
``tests/`` directory itself onto ``sys.path`` (rootdir-based import, not a
package), which makes that a plain top-level import rather than a relative
one.
"""

from __future__ import annotations

from types import SimpleNamespace


def render_fields(op, **conf) -> None:
    """Render ``op``'s templated fields, using ``conf`` as ``dag_run.conf``.

    No ``DAG``/``DagBag`` is constructed: ``op`` is an ordinary operator
    instance not bound to a DAG, and ``AbstractOperator.get_template_env()``
    falls back to a bare ``SandboxedEnvironment(cache_size=0)`` when
    ``op.dag`` is ``None`` — no Airflow DB access, no serialization.
    """
    op.render_template_fields(context={"dag_run": SimpleNamespace(conf=conf)})
