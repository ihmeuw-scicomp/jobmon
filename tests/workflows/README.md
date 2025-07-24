# Jobmon Client Development Workflows

This directory contains workflow scripts for testing and developing with jobmon_client. It's mounted as a volume in the `jobmon_client` Docker container for interactive development and testing.

## Getting Started

### 1. Start the Services

```bash
# Start all services (backend, frontend, and client)
docker-compose up -d

# Or start just the client and backend
docker-compose up -d jobmon_backend jobmon_client
```

### 2. Access the Client Container

```bash
# Open an interactive shell in the client container
docker-compose exec jobmon_client bash

# Or run a script directly
docker-compose exec jobmon_client python sample_workflow.py
```

### 3. Available Directories in the Container

- `/app/test_scripts/` - Your development workflows (this directory: `tests/workflows/`)
- `/app/example_scripts/` - Example scripts from `tests/_scripts/`
- `/app/quickstart_examples/` - Quickstart examples from documentation
- `/app/jobmon_core/` - Jobmon core source code
- `/app/jobmon_client/` - Jobmon client source code

## Sample Scripts

### sample_workflow.py
A basic example showing how to create a simple workflow with task dependencies. Modify this script to test different workflow scenarios.

## Creating Your Own Test Scripts

1. Create new Python scripts in `tests/workflows/` (this directory)
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

## Examples to Try

```bash
# Inside the container, try these examples:
cd /app/example_scripts
python memory_usage_array.py

cd /app/quickstart_examples  
python data_prep.py
```

## Configuration

The container uses the same configuration as the backend:
- Config file: `/app/config/jobmonconfig.local.yaml`
- Auth disabled for local development
- Logs to stdout with DEBUG level 