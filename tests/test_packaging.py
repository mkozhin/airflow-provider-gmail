"""Packaging smoke test (Task 15b).

Proves that the *built distribution* — not just the working tree — carries the
package and its ``apache_airflow_provider`` entry point, and that a clean
interpreter can discover the Gmail connection type through ``ProvidersManager``.

Marked ``@pytest.mark.packaging`` (registered in ``pyproject.toml``) and excluded
from the default run via ``addopts = "-m 'not packaging'"``. Run it explicitly:

    pytest -m packaging tests/test_packaging.py

The whole thing is driven from *one* pytest test (no shell/workflow duplication):

  1. ``python -m build``           -> sdist + wheel
  2. ``twine check dist/*``        -> metadata is valid
  3. fresh venv:
       ``pip install "apache-airflow==2.9.1" --constraint <constraints-2.9.1>``
       (pin the runtime first, else the wheel's ``>=2.9,<3`` pulls Airflow 2.11
       and the smoke test would validate the wrong version)
     then install the built wheel keeping the same pin
  4. in that venv:
       - ``import airflow_provider_gmail; get_provider_info()`` has connection-types
       - ``ProvidersManager()`` finds conn-type ``gmail`` and loads ``GmailHook``
       - ``airflow providers list`` contains the package

The build + twine steps run whenever possible. The wheel-install step needs
network (PyPI + the Airflow constraints file); when that is unavailable the test
``pytest.skip``s with a clear reason instead of failing spuriously. In CI the
network is present and the full path runs.
"""

from __future__ import annotations

import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

import pytest

pytestmark = pytest.mark.packaging

# Pin the runtime to the exact target Airflow with its official constraints file
# for the *running* Python. The suffix must match the interpreter, else the URL
# resolves to a non-existent constraints file.
AIRFLOW_PIN = "apache-airflow==2.9.1"
CONSTRAINTS_URL = (
    "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/"
    f"constraints-{sys.version_info.major}.{sys.version_info.minor}.txt"
)

# Long budget: installing Airflow 2.9.1 into a clean venv pulls many wheels.
_INSTALL_TIMEOUT = 900


def _online(url: str = CONSTRAINTS_URL) -> bool:
    """Best-effort reachability probe for the constraints file / PyPI."""
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            return resp.status == 200
    except (urllib.error.URLError, OSError, ValueError):
        return False


def _run(cmd: list[str], *, cwd: Path | None = None, timeout: int = 300) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _fail(step: str, proc: subprocess.CompletedProcess) -> None:
    pytest.fail(
        f"{step} failed (exit {proc.returncode})\n"
        f"--- stdout ---\n{proc.stdout}\n--- stderr ---\n{proc.stderr}"
    )


VERIFY_SNIPPET = """
import sys
import airflow_provider_gmail
from airflow_provider_gmail import get_provider_info

info = get_provider_info()
assert "connection-types" in info, "get_provider_info() lacks connection-types"
conn_types = info["connection-types"]
assert any(c.get("connection-type") == "gmail" for c in conn_types), conn_types

from airflow.providers_manager import ProvidersManager

hooks = ProvidersManager().hooks
assert "gmail" in hooks, f"gmail conn-type not found; have {sorted(hooks)}"
hook_info = hooks["gmail"]
assert hook_info is not None, "gmail hook failed to import in ProvidersManager"
assert hook_info.hook_class_name == "airflow_provider_gmail.hooks.gmail.GmailHook", hook_info.hook_class_name

# Prove the class actually imports from the installed wheel, not just the string.
from airflow_provider_gmail.hooks.gmail import GmailHook

assert GmailHook.__name__ == "GmailHook"
print("VERIFY_OK")
"""


def test_packaging_smoke(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parent.parent
    dist_dir = tmp_path / "dist"

    # 1. Build sdist + wheel from the working tree.
    build = _run(
        [sys.executable, "-m", "build", "--outdir", str(dist_dir), str(repo_root)],
        timeout=600,
    )
    if build.returncode != 0:
        # Build isolation fetches setuptools/setuptools-scm from PyPI; a network
        # outage surfaces here. Skip rather than fail in that case.
        if not _online("https://pypi.org/simple/setuptools-scm/"):
            pytest.skip(
                "no network: 'python -m build' could not fetch build deps "
                f"(runs in CI). Build output:\n{build.stderr}"
            )
        _fail("python -m build", build)

    wheels = list(dist_dir.glob("*.whl"))
    sdists = list(dist_dir.glob("*.tar.gz"))
    assert wheels, f"no wheel produced in {dist_dir}: {list(dist_dir.iterdir())}"
    assert sdists, f"no sdist produced in {dist_dir}: {list(dist_dir.iterdir())}"
    wheel = wheels[0]

    # 2. twine check on everything in dist/.
    twine = _run(
        [sys.executable, "-m", "twine", "check", *[str(p) for p in dist_dir.glob("*")]],
        timeout=120,
    )
    if twine.returncode != 0:
        _fail("twine check dist/*", twine)

    # 3+4. The wheel-install verification needs the network. Skip cleanly if the
    # constraints file / PyPI is unreachable; build + twine above already ran.
    if not _online():
        pytest.skip(
            "no network for wheel-install step: constraints file unreachable "
            f"({CONSTRAINTS_URL}). build + twine check passed; full step runs in CI."
        )

    # Fresh, isolated venv.
    venv_dir = tmp_path / "venv"
    created = _run([sys.executable, "-m", "venv", str(venv_dir)], timeout=120)
    if created.returncode != 0:
        _fail("python -m venv", created)

    if sys.platform == "win32":  # pragma: no cover - CI/dev are POSIX
        venv_python = venv_dir / "Scripts" / "python.exe"
        venv_airflow = venv_dir / "Scripts" / "airflow.exe"
    else:
        venv_python = venv_dir / "bin" / "python"
        venv_airflow = venv_dir / "bin" / "airflow"

    # Modern pip so Airflow's constraints resolve cleanly.
    up = _run(
        [str(venv_python), "-m", "pip", "install", "--upgrade", "pip"],
        timeout=_INSTALL_TIMEOUT,
    )
    if up.returncode != 0:
        _fail("pip install --upgrade pip", up)

    # Pin the runtime FIRST so the wheel's `>=2.9,<3` cannot pull 2.11.
    pin = _run(
        [
            str(venv_python), "-m", "pip", "install",
            AIRFLOW_PIN, "--constraint", CONSTRAINTS_URL,
        ],
        timeout=_INSTALL_TIMEOUT,
    )
    if pin.returncode != 0:
        _fail(f"pip install {AIRFLOW_PIN} (constrained)", pin)

    # Install the built wheel keeping the same constraint pin.
    inst = _run(
        [
            str(venv_python), "-m", "pip", "install",
            str(wheel), "--constraint", CONSTRAINTS_URL,
        ],
        timeout=_INSTALL_TIMEOUT,
    )
    if inst.returncode != 0:
        _fail("pip install <wheel> (constrained)", inst)

    # Confirm the runtime pin survived the wheel install.
    ver = _run(
        [
            str(venv_python), "-c",
            "import airflow; print(airflow.__version__)",
        ],
        timeout=120,
    )
    assert ver.returncode == 0, ver.stderr
    assert ver.stdout.strip() == "2.9.1", (
        f"expected Airflow 2.9.1, got {ver.stdout.strip()!r} — pin leaked"
    )

    # get_provider_info() + ProvidersManager discovery in the clean interpreter.
    verify = _run([str(venv_python), "-c", VERIFY_SNIPPET], timeout=180)
    if verify.returncode != 0 or "VERIFY_OK" not in verify.stdout:
        _fail("in-venv provider discovery", verify)

    # `airflow providers list` must mention the package.
    airflow_home = tmp_path / "airflow_home"
    env = {
        "AIRFLOW_HOME": str(airflow_home),
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
        "PATH": str(venv_python.parent),
    }
    providers = subprocess.run(
        [str(venv_airflow), "providers", "list", "--output", "plain"],
        capture_output=True,
        text=True,
        timeout=300,
        env=env,
    )
    if providers.returncode != 0:
        _fail("airflow providers list", providers)
    assert "airflow-provider-gmail" in providers.stdout, (
        "package not listed by 'airflow providers list':\n" + providers.stdout
    )
