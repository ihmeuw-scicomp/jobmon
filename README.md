# JobMon

## Table of Contents

- [Introduction](#introduction)
- [Description](#description)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Requirements](#requirements)
- [Documentation](#documentation)
- [Branching Strategy](#branching-strategy)
- [Contributing](#contributing)
- [Changelog](#changelog)
- [License](#license)

## Introduction

JobMon is a Python package developed by IHME's Scientific Computing team, designed to simplify and standardize the process of job monitoring and workflow management in computational projects. It facilitates the tracking of job statuses, manages dependencies, and streamlines the execution of complex workflows across various computing environments.

## Description

The tool aims to enhance productivity and ensure computational tasks are efficiently managed and executed, offering a robust solution for handling large-scale, data-driven analyses in research and development projects.

## Features

- **Workflow Management**: Easily define and manage workflows with multiple interdependent tasks.
- **Status Tracking**: Real-time tracking of job statuses to monitor the progress of computational tasks.
- **Error Handling**: Automatically detect and report errors in jobs, supporting swift resolution and rerun capabilities.
- **Compatibility**: Designed to work seamlessly across different computing environments, including HPC clusters and cloud platforms.

## Installation

To install JobMon, use the following pip command:

```bash
pip install jobmon_client[server]
```

## Usage

Refer to the [quickstart](https://jobmon.readthedocs.io/en/latest/quickstart.html#create-a-workflow) to get started with a sample workflow

## Requirements

- Python 3.8+
- uv (Python package manager, https://github.com/astral-sh/uv)

## Documentation

For comprehensive documentation, visit [readthedocs](https://jobmon.readthedocs.io/en/latest/#).

## Developer Setup

Contributing to JobMon? Here's how to set up your environment. This project utilizes `uv` for Python package management, Docker for containerization, and Nox for task automation. The root `pyproject.toml` defines the workspace structure for `uv`.

**Prerequisites:**
1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-org/jobmon.git # Replace with your actual repo URL
    cd jobmon
    ```
2.  **Install `uv`:**
    Follow the official `uv` installation instructions (e.g., `pip install uv` or `curl -LsSf https://astral.sh/uv/install.sh | sh`).

**Configuring IHME Artifactory Index (Conditional):**
This step is **crucial if you plan to generate or update `uv.lock` files** (e.g., using `nox -s update_locks`, or `uv pip compile` commands). If you are only using existing `uv.lock` files for tasks like running tests or linting, and all dependencies are publicly mirrored or vendored, you might not need this.
*   The recommended way is to create or update your `pip.conf` file:
    *   Linux/macOS: `~/.config/pip/pip.conf` (preferred) or `~/.pip/pip.conf`
    *   Windows: `%APPDATA%\pip\pip.ini` or `%USERPROFILE%\pip\pip.ini`
*   Add the following content:
    ```ini
    [global]
    extra-index-url = https://artifactory.ihme.washington.edu/artifactory/api/pypi/pypi-shared/simple
    # trusted-host = artifactory.ihme.washington.edu # May be needed if not HTTPS or cert issues
    ```
*   Alternatively, set the `UV_EXTRA_INDEX_URL` environment variable specifically when needed:
    ```bash
    export UV_EXTRA_INDEX_URL="https://artifactory.ihme.washington.edu/artifactory/api/pypi/pypi-shared/simple"
    ```

**Development Workflows:**

Choose one of the following workflows:

1.  **Nox Workflow Automation (Recommended):**
    Nox (via `noxfile.py`) provides the most straightforward way to manage common development tasks. It uses `uv` to create isolated environments, ensuring consistency.
    *   **Setup:**
        *   Install Nox: `uv pip install nox` (or `pip install nox`).
    *   **Usage:**
        *   List available sessions: `nox -l`
        *   Run specific sessions: `nox -s tests lint format`
        *   The `nox -s update_locks` session requires the IHME Artifactory Index configuration (see above).
    *   Refer to the "Nox Sessions" section below for a detailed list of available commands and their functions.

2.  **Docker-based Development:**
    Use Docker and the provided `docker-compose.yml` for a fully containerized development environment. This is ideal for ensuring consistency across different machines or for mimicking production-like setups.
    *   **Setup:**
        *   Ensure Docker and Docker Compose are installed.
        *   (Optional) Create a `.env` file in the root directory if specific environment variables are needed for the services (e.g., database configurations). The `docker-compose.yml` references an `.env` file.
    *   **Services defined in `docker-compose.yml`:**
        *   `jobmon_backend`: Runs the FastAPI server (from `jobmon_server`) with hot reloading. Code from `jobmon_core` and `jobmon_server` is mounted.
        *   `jobmon_frontend`: Runs the GUI (from `jobmon_gui`). Source code is mounted.
    *   **Usage:**
        *   Start all services: `docker-compose up` (add `-d` for detached mode).
        *   Stop services: `docker-compose down`.
        *   View logs for a specific service: `docker-compose logs jobmon_backend`.

3.  **Manual `uv` Workspace Setup (for IDEs or Custom Workflows):**
    If you prefer a single, manually managed Python environment for the entire workspace:
    *   **Setup:**
        1.  Ensure `uv` is installed.
        2.  Configure the IHME Artifactory Index (see above) if you plan to modify dependencies or (re)generate lock files.
        3.  From the repository root, create and activate a virtual environment:
            ```bash
            uv venv .venv
            source .venv/bin/activate  # Or .venv\Scripts\activate on Windows
            ```
        4.  Install all workspace projects in editable mode:
            ```bash
            uv pip install -e ./jobmon_core -e ./jobmon_client -e ./jobmon_server
            ```
            This installs the sub-projects and their direct dependencies as defined in their respective `pyproject.toml` files, using your local source code. This setup works seamlessly because the individual `pyproject.toml` files (e.g., in `jobmon_client`) use `[tool.uv.sources] <dependency_name> = { workspace = true }` to instruct `uv` to use the local versions of other workspace packages. For most development tasks (testing, linting, etc.), using the Nox workflow (Option 1) is still recommended as it handles isolated environments and tool installations more robustly.

### Nox Sessions

The `noxfile.py` in this project defines several sessions for common development tasks:

*   `nox -s tests`: Runs the test suite using pytest.
*   `nox -s lint`: Lints the codebase using flake8 and associated plugins to check for style, import order, docstrings, and annotations.
*   `nox -s format`: Formats the code using black (for code style), isort (for import sorting), and autoflake (to remove unused imports).
*   `nox -s typecheck`: Performs static type checking using mypy.
*   `nox -s schema_diagram`: Generates an Entity Relationship Diagram (ERD) for the database schema using schemacrawler and Docker. The output is saved to `docsource/developers_guide/diagrams/erd.svg`.
*   `nox -s build`: Builds the Python packages for each sub-project (`jobmon_client`, `jobmon_core`, `jobmon_server`).
*   `nox -s clean`: Removes all build artifacts, caches (like `.pytest_cache`, `.mypy_cache`), `.egg-info` directories, and virtual environments (`.venv`).
*   `nox -s build_gui_test_env`: Prepares the environment for GUI testing by installing necessary dependencies and setting up a test database.
*   `nox -s update_locks`: Generates/updates `uv.lock` files for all sub-projects (`jobmon_core`, `jobmon_client`, `jobmon_server`) based on their `pyproject.toml` files. This ensures reproducible dependencies. **Requires the IHME Artifactory index to be configured (see step 3 under Developer Setup) if private packages are needed.**

You can run a specific session using `nox -s <session_name>`. To list all available sessions, use `nox -l`.

## Usage

Refer to the [quickstart](https://jobmon.readthedocs.io/en/latest/quickstart.html#create-a-workflow) to get started with a sample workflow

## Contributing

We encourage contributions from the community. If you're interested in improving JobMon or adding new features, please refer to our [developer guide](https://jobmon.readthedocs.io/en/latest/developers_guide/developer-start.html) for python client contributions or the GUI [README.md](jobmon_gui/README.md) for visualization contributions. The local development setup now uses `uv` and `nox` as described in the "Developer Setup" section.

## Branching Strategy

This project utilizes a branching strategy that emphasizes release branches and semantic versioning, facilitating orderly development, feature addition, bug fixes, and updates.

### Overview

- **Main Branch**: The `main` branch maintains the latest stable release of the project. It represents the culmination of all development efforts into a stable version ready for production use.
- **Release Branches**: These branches, named according to semantic versioning as `release/X.Y`, host development for upcoming minor or major releases. When starting a new major or minor version, it is branched off from the latest stable version in `main`.
- **Feature and Bug Fix Branches**: Development of new features and bug fixes happens in branches derived from the appropriate `release/X.Y` branches. Once development is complete, reviewed, and tested, these changes are merged back into their respective `release/X.Y` branch.

### Semantic Versioning

We adopt [semantic versioning](https://semver.org/) for organizing our releases:

- **Major Version (X)**: Incremented for significant changes or incompatible API modifications.
- **Minor Version (Y)**: Incremented for backward-compatible enhancements.
- **Patch Version (Z)**: Incremented for backward-compatible bug fixes.

### Development and Release Process

1. **Starting New Features and Fixes**: Branch off from the corresponding `release/X.Y` branch for developing new features or addressing bugs. Ensure your branch name clearly reflects the purpose of the changes.
2. **Applying Bug Fixes**: If a bug fix applies to a release branch that has diverged from `main`, it should first be applied to the most current release branch where relevant, before merging into the specific `release/X.Y` branch.
3. **Pull Request (PR)**: Submit a PR against the `release/X.Y` branch from which you branched out. The PR must summarize the changes and include any pertinent information for the reviewers.
4. **Creating Tags and Merging to Main**:
    - Upon completion of a release cycle, a version tag following the `X.Y.Z` format is created for the `release/X.Y` branch.
    - This tagged `release/X.Y` branch is then merged into `main`, signifying the release of a new version.

## Changelog

For a detailed history of changes and version updates, please refer to the [CHANGELOG.md](CHANGELOG.md) file within this repository.

## License

This project is licensed under the JobMon Non-commercial License, developed at the Institute for Health Metrics and Evaluation (IHME), University of Washington. The license allows for redistribution and use in source and binary forms, with or without modification, under the conditions that:

- The software is used solely for non-commercial purposes. Commercial use, including indirect commercial use such as content on a website that accepts advertising money, is not permitted under this license. However, use by a for-profit company in its research is considered non-commercial use.
- All redistributions of source code must retain the copyright notice, this list of conditions, and the following disclaimer.
- Redistributions in binary form must reproduce the copyright notice, this list of conditions, and the following disclaimer in the documentation and/or other materials provided with the distribution.
- Neither the name of the University of Washington nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

For commercial use rights, contact the University of Washington, CoMotion, at license@uw.edu or call 206-543-3970, and inquire about this project.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

For a full copy of the license, see the [LICENSE](LICENSE) file in this repository.
