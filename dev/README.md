# Jobmon Local Development

This directory contains everything needed for local development with Docker.

## Directory Structure

```
dev/
├── README.md           # This file
├── config/             # Configuration files for Docker containers
│   ├── jobmonconfig.example.yaml      # Example main config (copy to .local.yaml)
│   ├── jobmonconfig.local.yaml        # Your local config (gitignored)
│   ├── logconfig.example.yaml         # Basic logging (OTLP disabled)
│   ├── logconfig.otlp.example.yaml    # OTLP logging example
│   └── logconfig.otlp.yaml            # Your OTLP config (gitignored)
├── workflows/          # Test workflow scripts for jobmon_client container
│   ├── six_job_test.py
│   ├── six_job_structlog_test.py
│   └── six_job_test_resources.yaml
└── scripts/            # Helper scripts (future)
```

## Quick Start

### 1. Set Up Configuration

```bash
# Copy example configs if you don't have local configs yet
cd dev/config/
cp jobmonconfig.example.yaml jobmonconfig.local.yaml
cp logconfig.otlp.example.yaml logconfig.otlp.yaml

# Edit configs as needed for your environment
```

### 2. Initialize the Database

```bash
# From project root, using your local venv
source .venv/bin/activate
JOBMON__CONFIG_FILE=dev/config/jobmonconfig.local.yaml jobmon_server init_db

# Or using Make (if available)
make init-db
```

### 3. Start Services

```bash
# Start all services
docker compose up -d

# Or start specific services
docker compose up -d jobmon_backend jobmon_client
```

### 4. Access Services

- **Backend API**: http://localhost:8070/api/v3
- **Frontend GUI**: http://localhost:3000
- **API Health**: http://localhost:8070/api/v3/health

### 5. Run Test Workflows

```bash
# Open shell in client container
docker compose exec jobmon_client bash

# Inside the container, run a test workflow
cd /app/test_scripts
python six_job_test.py sequential
```

## Common Tasks

### View Logs

```bash
# Follow all service logs
docker compose logs -f

# Follow specific service
docker compose logs -f jobmon_backend
```

### Restart Services

```bash
# Restart with config changes (no rebuild needed)
docker compose restart jobmon_backend

# Full rebuild (if dependencies change)
docker compose up --build -d
```

### Database Reset

```bash
# Remove the SQLite database and reinitialize
rm -f jobmon_server/jobmon.db
JOBMON__CONFIG_FILE=dev/config/jobmonconfig.local.yaml jobmon_server init_db
```

### Run Tests

```bash
# Unit tests (no server needed)
pytest tests/unit/ -x

# Integration tests (need server running)
docker compose up -d jobmon_backend
pytest tests/integration/ --tb=short
```

## Configuration Files

### jobmonconfig.local.yaml

Main Jobmon configuration. Key sections:

- **db**: Database connection (SQLite for local dev)
- **http**: API endpoint configuration
- **logging**: Paths to logconfig files
- **telemetry**: OTLP tracing and logging exporters
- **auth**: Disabled for local development

### logconfig.otlp.yaml

Python logging configuration with OTLP support. Features:

- Console output with structlog formatting
- OTLP handlers for distributed tracing
- Per-component log levels

## Troubleshooting

### Config file not found

Ensure you've copied the example files:
```bash
ls -la dev/config/
# Should show both .example.yaml and .local.yaml files
```

### Database connection issues

Check SQLite file exists and path is correct:
```bash
ls -la jobmon_server/jobmon.db
```

### Container can't find config

Verify the volume mount in docker-compose.yml points to `./dev/config`.

### OTLP connection issues

The default configs point to IHME's OTLP collector. For local-only development:
1. Use `logconfig.example.yaml` instead (OTLP disabled)
2. Or update endpoints in `logconfig.otlp.yaml` to your local collector

## Migration from docker_config/

If you were using the old `docker_config/` location:

```bash
# Your configs are automatically preserved - the old location still works
# but new development should use dev/config/

# To migrate:
mv docker_config/jobmonconfig.local.yaml dev/config/
mv docker_config/logconfig.otlp.yaml dev/config/
```

