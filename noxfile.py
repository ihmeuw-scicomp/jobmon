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


@nox.session(venv_backend="venv")
def tests(session: Session) -> None:
    """Run the test suite."""
    session.install("uv")
    # Install all dev dependencies, including pytest and workspace packages
    session.run("uv", "sync", "--active", "--group", "dev")

    args = session.posargs or test_locations

    session.run(
        "coverage",
        "run",
        "-m",
        "pytest",
        "--junitxml=.test_report.xml",
        *args,
        env={"SQLALCHEMY_WARN_20": "1"}
    )


@nox.session(venv_backend="venv")
def lint(session: Session) -> None:
    """Lint code using various plugins.

    flake8 - a Python library that wraps PyFlakes, pycodestyle and McCabe script.
    flake8-import-order - checks the ordering of your imports.
    flake8-docstrings - extension for flake8 which uses pydocstyle to check docstrings.
    flake8-annotations -is a plugin for Flake8 that detects the absence of PEP 3107-style
    function annotations and PEP 484-style type comments.
    """
    session.install("uv")
    args = session.posargs or src_locations
    # Install all dev dependencies, including linters and workspace packages
    session.run("uv", "sync", "--active", "--group", "dev")
    session.run("flake8", *args)


@nox.session(venv_backend="venv")
def format(session):
    session.install("uv")
    args = session.posargs or src_locations + test_locations
    # Install all dev dependencies, including formatters and workspace packages
    session.run("uv", "sync", "--active", "--group", "dev")
    session.run(
        "autoflake",
        "--in-place",
        "--remove-all-unused-imports",
        "--recursive",
        *args
    )
    session.run("isort", *args) 
    session.run("black", *args)


@nox.session(venv_backend="venv")
def typecheck(session: Session) -> None:
    """Type check code."""
    session.install("uv")
    args = session.posargs or src_locations
    # Install all dev dependencies, including mypy, types, and workspace packages
    session.run("uv", "sync", "--active", "--group", "dev")
    session.run("mypy", "--explicit-package-bases", *args)


@nox.session(venv_backend="venv")
def schema_diagram(session: Session) -> None:
    session.install("uv")
    # This session specifically installs jobmon_server.
    # If jobmon_server and its dependencies are part of 'dev' extras,
    # 'uv sync --active --extra dev' would also work here.
    # For now, keeping it specific if it has unique needs beyond general dev.
    # If jobmon_server is already in dev dependencies, this could be:
    session.run("uv", "pip", "install", "--active", "-e", "./jobmon_server")
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


@nox.session(venv_backend="venv")
def build(session: Session) -> None:
    session.install("uv")
    args = session.posargs or src_locations
    # Install build tool and ensure workspace packages are available
    session.run("uv", "sync", "--active", "--group", "dev")

    for src_dir in args:
        namespace_dir = str(Path(src_dir).parent)
        session.run("python", "-m", "build", "--outdir", "dist", namespace_dir)


@nox.session(venv_backend="venv")
def clean(session: Session) -> None:
    """Clean up all build artifacts, caches, and local virtual environments."""
    session.log("Removing build artifacts, caches, and .egg-info directories...")
    dirs_to_remove = [
        'out', 'dist', '.eggs',
        '.pytest_cache', 'docsource/api', '.mypy_cache',
        '.venv',  # If you have a root .venv
    ]
    
    # Add sub-project specific .venv directories
    for project_root_name in ["jobmon_client", "jobmon_core", "jobmon_server"]:
        project_path = Path(project_root_name)
        if project_path.is_dir(): # Ensure the base project dir exists
            venv_path = project_path / ".venv"
            if venv_path.exists() and venv_path.is_dir():
                dirs_to_remove.append(str(venv_path))

    # Broader .egg-info and build patterns
    # Using Path.rglob to find these recursively might be more robust
    # For simplicity, keeping glob for now but being more specific
    # Add root patterns first
    if Path("build").exists(): dirs_to_remove.append("build")
    if Path(".coverage").exists(): os.remove(".coverage") # direct file removal
    if Path(".test_report.xml").exists(): os.remove(".test_report.xml") # direct file removal

    # Using rglob for .egg-info, jobmon_*/build, etc.
    repo_root = Path(".")
    for egg_info_path in repo_root.rglob("*.egg-info"):
        if egg_info_path.is_dir():
            dirs_to_remove.append(str(egg_info_path))
    
    for build_dir_path in repo_root.glob("jobmon_*/build"):
        if build_dir_path.is_dir():
            dirs_to_remove.append(str(build_dir_path))

    # Remove duplicate strings that might have been added
    unique_dirs_to_remove = sorted(list(set(dirs_to_remove)))

    for path_str in unique_dirs_to_remove:
        path_obj = Path(path_str)
        if path_obj.exists():
            if path_obj.is_dir():
                session.log(f"Removing directory: {path_obj}")
                shutil.rmtree(path_obj, ignore_errors=True)
            elif path_obj.is_file(): # Should not happen with current list, but good practice
                session.log(f"Removing file: {path_obj}")
                try:
                    path_obj.unlink()
                except OSError as e:
                    session.warn(f"Could not remove file {path_obj}: {e}")
    
    # Files that might not be caught by directory removals or are at root
    session.log("Removing specific root files if they exist...")
    files_at_root_to_remove = ['.test_report.xml', '.coverage']
    for file_str in files_at_root_to_remove:
        file_obj = Path(file_str)
        if file_obj.exists() and file_obj.is_file():
            session.log(f"Removing file: {file_obj}")
            try:
                file_obj.unlink()
            except OSError as e:
                session.warn(f"Could not remove file {file_obj}: {e}")

    session.log("Clean-up finished.")


@nox.session(venv_backend="venv")
def build_gui_test_env(session: Session) -> None:
    session.install("uv")
    # Install dev dependencies which now include mysqlclient and workspace packages
    session.run("uv", "sync", "--active", "--group", "dev")
    if os.path.exists("/tmp/tests.sqlite"):
        os.remove("/tmp/tests.sqlite")
    # The workspace packages (jobmon_core, jobmon_client, jobmon_server)
    # are already installed as part of 'uv sync --active --extra dev'


# New session to update uv.lock files
@nox.session(venv_backend="venv") # Runs with the Python nox is invoked with, uv handles target Python for compile
def update_locks(session: Session) -> None:
    """Generate uv.lock files for all sub-projects."""
    session.install("uv")

    # Ensure UV_EXTRA_INDEX_URL is set in the environment
    # For example, export UV_EXTRA_INDEX_URL=https://artifactory.ihme.washington.edu/artifactory/api/pypi/pypi-shared/simple
    extra_index_url = os.environ.get("UV_EXTRA_INDEX_URL")
    if not extra_index_url:
        session.warn(
            "UV_EXTRA_INDEX_URL environment variable is not set. "
            "Lock file generation might fail or miss private packages."
        )
        # Optionally, you could make this an error: session.error("UV_EXTRA_INDEX_URL is required")
        # Or proceed, and let uv fail if the index is truly needed and not found in global pip.conf

    python_target_version = "3.12" # Align with production Dockerfile Python version
    compile_args = [
        "--all-extras",
        f"--python={python_target_version}"
    ]
    if extra_index_url:
        compile_args.extend(["--extra-index-url", extra_index_url])

    projects = {
        "jobmon_core": "jobmon_core/pyproject.toml",
        "jobmon_client": "jobmon_client/pyproject.toml",
        "jobmon_server": "jobmon_server/pyproject.toml",
    }

    session.log(
        "Compiling lock files for each project. UV will use the `[tool.uv.sources]` "
        "declarations in individual `pyproject.toml` files to resolve local workspace "
        "dependencies correctly."
    )
    for _, pyproject_path in projects.items():
        lock_path = str(Path(pyproject_path).parent / "uv.lock")
        session.log(f"Generating {lock_path} from {pyproject_path}...")
        session.run(
            "uv", "pip", "compile", pyproject_path, 
            "-o", lock_path, 
            *compile_args
        )
    session.log("All uv.lock files updated successfully.")


@nox.session(venv_backend="venv")
def generate_api_types(session: Session) -> None:
    """Generate TypeScript types from the FastAPI backend's OpenAPI schema using npx."""

    backend_url = os.environ.get("JOBMON_BACKEND_URL", "http://localhost:8070")
    # Adjusted path to match FastAPI app's openapi_url configuration
    openapi_schema_path = f"{backend_url}/api/openapi.json" 
    output_file = str(Path("jobmon_gui/src/types/apiSchema.ts"))

    session.log(f"Generating TypeScript types from {openapi_schema_path} to {output_file} using npx...")
    
    output_dir = Path(output_file).parent
    if not output_dir.exists():
        session.log(f"Creating output directory: {output_dir}")
        output_dir.mkdir(parents=True, exist_ok=True)

    try:
        session.run(
            "npx",
            "openapi-typescript",
            openapi_schema_path,
            "--output",
            output_file,
            external=True
        )
        session.log(f"TypeScript API types generated successfully at {output_file}")
    except nox.command.CommandFailed as e:
        session.error(
            f"Failed to generate API types. Ensure Node.js and npx are installed and in your PATH. "
            f"The backend must be running and accessible at {openapi_schema_path}. "
            f"Original error: {e}"
        )


@nox.session(venv_backend="venv")
def lint_frontend(session: Session) -> None:
    """Lint frontend TypeScript/React code."""
    session.log("Running frontend linting...")
    session.chdir("jobmon_gui")
    
    # Check if node_modules exists, if not install dependencies
    if not Path("node_modules").exists():
        session.log("Installing frontend dependencies...")
        session.run("npm", "install", external=True)
    
    # Run TypeScript type checking
    session.log("Running TypeScript type checking...")
    session.run("npm", "run", "typecheck", external=True)
    
    # Run ESLint
    session.log("Running ESLint...")
    session.run("npm", "run", "lint", external=True)


@nox.session(venv_backend="venv")
def typecheck_frontend(session: Session) -> None:
    """Type check frontend TypeScript code."""
    session.log("Running frontend type checking...")
    session.chdir("jobmon_gui")
    
    # Check if node_modules exists, if not install dependencies
    if not Path("node_modules").exists():
        session.log("Installing frontend dependencies...")
        session.run("npm", "install", external=True)
    
    # Run TypeScript type checking
    session.run("npm", "run", "typecheck", external=True)


@nox.session(venv_backend="venv")
def format_frontend(session: Session) -> None:
    """Format frontend TypeScript/React code."""
    session.log("Running frontend formatting...")
    session.chdir("jobmon_gui")
    
    # Check if node_modules exists, if not install dependencies
    if not Path("node_modules").exists():
        session.log("Installing frontend dependencies...")
        session.run("npm", "install", external=True)
    
    # Run prettier
    session.run("npm", "run", "format", external=True)


@nox.session(venv_backend="venv")
def lint_all(session: Session) -> None:
    """Lint both backend and frontend code."""
    session.log("Running linting for both backend and frontend...")
    
    # Run backend linting
    session.log("=== Backend Linting ===")
    session.notify("lint")
    
    # Run frontend linting
    session.log("=== Frontend Linting ===")
    session.notify("lint_frontend")


@nox.session(venv_backend="venv")
def typecheck_all(session: Session) -> None:
    """Type check both backend and frontend code."""
    session.log("Running type checking for both backend and frontend...")
    
    # Run backend type checking
    session.log("=== Backend Type Checking ===")
    session.notify("typecheck")
    
    # Run frontend type checking
    session.log("=== Frontend Type Checking ===")
    session.notify("typecheck_frontend")


@nox.session(venv_backend="venv")
def format_all(session: Session) -> None:
    """Format both backend and frontend code."""
    session.log("Running formatting for both backend and frontend...")
    
    # Run backend formatting
    session.log("=== Backend Formatting ===")
    session.notify("format")
    
    # Run frontend formatting
    session.log("=== Frontend Formatting ===")
    session.notify("format_frontend")
