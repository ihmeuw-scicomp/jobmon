"""Nox Configuration for Jobmon."""
import glob
import os
from pathlib import Path
import shutil
import tempfile

import nox
from nox.sessions import Session

src_locations = ["jobmon_client/src", "jobmon_core/src", "jobmon_server/src"]
test_locations = ["tests"]

python = "3.10"


@nox.session(python=python, venv_backend="venv")
def tests(session: Session) -> None:
    """Run the test suite."""
    session.run("uv", "pip", "install", "pytest", "pytest-xdist", "pytest-cov", "mock", "filelock", "pytest-mock")
    session.run("uv", "pip", "install", "-e", "./jobmon_core", "-e", "./jobmon_client", "-e", "./jobmon_server")

    args = session.posargs or test_locations

    session.run(
        "coverage",
        "run",
        "-m",
        "pytest",
        *args,
        env={"SQLALCHEMY_WARN_20": "1"}
    )


@nox.session(python=python, venv_backend="venv")
def lint(session: Session) -> None:
    """Lint code using various plugins.

    flake8 - a Python library that wraps PyFlakes, pycodestyle and McCabe script.
    flake8-import-order - checks the ordering of your imports.
    flake8-docstrings - extension for flake8 which uses pydocstyle to check docstrings.
    flake8-annotations -is a plugin for Flake8 that detects the absence of PEP 3107-style
    function annotations and PEP 484-style type comments.
    """
    args = session.posargs or src_locations
    # TODO: work these in over time?
    # "darglint",
    # "flake8-bandit"
    session.run(
        "uv",
        "pip",
        "install",
        "flake8",
        "flake8-annotations",
        "flake8-import-order",
        "flake8-docstrings",
        "flake8-black"
    )
    session.run("flake8", *args)


@nox.session(python=python, venv_backend="venv")
def black(session):
    args = session.posargs or src_locations + test_locations
    session.run("uv", "pip", "install", "black")
    session.run("black", *args)


@nox.session(python=python, venv_backend="venv")
def typecheck(session: Session) -> None:
    """Type check code."""
    args = session.posargs or src_locations
    session.run(
        "uv",
        "pip",
        "install",
        "mypy",
        "types-Flask",
        "types-requests",
        "types-PyMySQL",
        "types-filelock",
        "types-PyYAML",
        "types-tabulate",
        "types-psutil",
        "types-Flask-Cors",
        "types-sqlalchemy-utils",
        "types-setuptools",
        "types-mysqlclient"
    )
    session.run("uv", "pip", "install", "-e", "./jobmon_core", "-e", "./jobmon_client", "-e", "./jobmon_server")

    session.run("mypy", "--explicit-package-bases", *args)


@nox.session(python=python, venv_backend="venv")
def schema_diagram(session: Session) -> None:
    session.run("uv", "pip", "install", "-e", "./jobmon_server")
    outpath = Path(__file__).parent / "docsource" / "developers_guide" / "diagrams" / "erd.svg"
    with tempfile.TemporaryDirectory() as tmpdir:
        session.chdir(tmpdir)
        session.run(
            "jobmon_server", "init_db",
            env={"JOBMON__DB__SQLALCHEMY_DATABASE_URI": "sqlite:///jobmon.db"}
        )
        session.run("docker", "pull", "schemacrawler/schemacrawler", external=True)
        session.run(
            "docker",
            "run",
            "--mount", f"type=bind,source={tmpdir},target=/home/schcrwlr/share",
            "--rm", "-it", "schemacrawler/schemacrawler",
            "/opt/schemacrawler/bin/schemacrawler.sh",
            "--server=sqlite",
            "--database=share/jobmon.db",
            "--info-level=standard",
            "--portable-names",
            "--command", "schema",
            "--output-format=svg",
            "--output-file=share/erd.svg",
            "--title", "Jobmon Database",
            external=True
        )
        session.run("cp", "erd.svg", str(outpath))


@nox.session(python=python, venv_backend="venv")
def build(session: Session) -> None:
    args = session.posargs or src_locations
    session.run("uv", "pip", "install", "build")

    for src_dir in args:
        namespace_dir = str(Path(src_dir).parent)
        session.run("python", "-m", "build", "--outdir", "dist", namespace_dir)


@nox.session(python=python, venv_backend="venv")
def clean(session: Session) -> None:
    dirs_to_remove = ['out', 'dist', 'build', ".eggs",
                      '.pytest_cache', 'docsource/api', '.mypy_cache']
    egg_info = glob.glob("jobmon_*/src/*.egg-info")
    dirs_to_remove.extend(egg_info)
    builds = glob.glob("jobmon_*/build")
    dirs_to_remove.extend(builds)

    for path in dirs_to_remove:
        if os.path.exists(path):
            shutil.rmtree(path)

    files_to_remove = ['test_report.xml', '.coverage']
    for file in files_to_remove:
        if os.path.exists(file):
            os.remove(file)


@nox.session(python=python, venv_backend="venv")
def build_gui_test_env(session: Session) -> None:
    session.run("uv", "pip", "install", "mysqlclient")
    if os.path.exists("/tmp/tests.sqlite"):
        os.remove("/tmp/tests.sqlite")
    session.run("uv", "pip", "install", "-e", "./jobmon_core", "-e", "./jobmon_client", "-e", "./jobmon_server")
