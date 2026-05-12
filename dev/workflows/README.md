# Jobmon Client Development Workflows

This directory contains workflow scripts for testing and developing with jobmon_client. It's mounted as a volume in the `jobmon_client` Docker container for interactive development and testing.

## Getting Started

### 1. Initialize the Database (SQLite)

```bash
# Initialize the SQLite database (only needed once)
source .venv/bin/activate
jobmon_server init_db --sqlalchemy_database_uri sqlite:///jobmon_server/jobmon.db

# Or use the config file approach
JOBMON__CONFIG_FILE=dev/config/jobmonconfig.local.yaml jobmon_server init_db
```

### 2. Start the Services

```bash
# Start all services (backend, frontend, and client)
docker compose up -d

# Or start just the client and backend
docker compose up -d jobmon_backend jobmon_client
```

### 3. Access the Client Container

```bash
# Open an interactive shell in the client container
docker compose exec jobmon_client bash

# Or run a script directly
docker compose exec jobmon_client python six_job_test.py sequential
```

### 3a. Alternative: Run from the Host venv

The client scripts can also run directly from your host venv against the
Dockerized backend. Because the host venv does not load
`dev/config/jobmonconfig.local.yaml`, you must point the client at the
host-published backend port in your script:

```python
import os
os.environ["JOBMON__HTTP__SERVICE_URL"] = "http://localhost:8070"
```

Then run:

```bash
uv run python dev/workflows/simple_test.py
```

(Inside the `jobmon_client` container the YAML config already sets
`service_url: http://jobmon_backend:80`, so no override is needed there.)

### 4. Available Directories in the Container

- `/app/test_scripts/` - Your development workflows (this directory: `dev/workflows/`)
- `/app/jobmon_core/` - Jobmon core source code
- `/app/jobmon_client/` - Jobmon client source code

## Sample Scripts

### six_job_test.py
A basic example showing how to create a simple workflow with task dependencies. Modify this script to test different workflow scenarios.

## Creating Your Own Test Scripts

1. Create new Python scripts in `dev/workflows/` (this directory)
2. Use the jobmon client API to create workflows and tasks
3. Test different scenarios like:
   - Task dependencies
   - Error handling and retries
   - Resource allocation
   - Array tasks
   - Different compute environments

## Environment

The client container is configured with:
- Editable installs of jobmon_core and jobmon_client
- Access to the jobmon backend API (if running)
- Debug logging enabled
- Interactive shell support (stdin/tty)

## Configuration

The container uses the same configuration as the backend:
- Config file: `/app/config/jobmonconfig.local.yaml`
- Auth disabled for local development
- Logs to stdout with INFO level 