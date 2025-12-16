# Local Development Configuration

This directory contains example configuration files for running Jobmon locally with docker-compose. You need to copy these examples and customize them for your environment.

## Files

**Example Files (copy and customize these):**
- `jobmonconfig.example.yaml` - Example main Jobmon configuration 
- `logconfig.example.yaml` - Example logging configuration with OTLP disabled
- `logconfig.otlp.example.yaml` - Example logging configuration with OTLP enabled

**Your Local Files (create these from examples):**
- `jobmonconfig.local.yaml` - Your customized main Jobmon configuration
- `logconfig.local.yaml` - Your customized logging configuration (OTLP disabled)  
- `logconfig.otlp.yaml` - Your customized logging configuration (OTLP enabled)

## Quick Start

1. **Copy and customize the example configurations:**
   ```bash
   cd dev/config/
   
   # Copy main config and customize for your environment
   cp jobmonconfig.example.yaml jobmonconfig.local.yaml
   
   # Copy basic logging config (OTLP disabled)
   cp logconfig.example.yaml logconfig.local.yaml
   
   # Optional: Copy OTLP logging config for testing observability
   cp logconfig.otlp.example.yaml logconfig.otlp.yaml
   ```

2. **Edit your local configs:**
   - Update database connection details in `jobmonconfig.local.yaml`
   - Adjust OTLP endpoints if using `logconfig.otlp.yaml`
   - Modify log levels and other settings as needed

3. **Start the services:**
   ```bash
   docker compose up --build
   ```

4. **Access the application:**
   - Backend API: http://localhost:8070/api/v3
   - Frontend: http://localhost:3000

## Configuration Files

### jobmonconfig.local.yaml

This is your main Jobmon configuration for local development. Copy from `jobmonconfig.example.yaml` and customize:

**Key sections to customize:**
- **Database:** Update `db.sqlalchemy_database_uri` with your database credentials
- **Logging:** Points to your logging config file (default: `logconfig.local.yaml`)
- **Telemetry:** Configure OTLP endpoints if using distributed tracing
- **CORS:** Adjust `cors.allowed_origins` if your frontend runs on a different port

**Example customizations:**
```yaml
# Use local database instead of production
db:
  sqlalchemy_database_uri: "mysql+mysqldb://root:password@localhost:3306/jobmon_dev"

# Use basic logging (no OTLP)
logging:
  server_logconfig_file: /app/config/logconfig.local.yaml
  
# Or enable OTLP logging for testing
logging:
  server_logconfig_file: /app/config/logconfig.otlp.yaml
```

### logconfig.local.yaml (Default - OTLP Disabled)

Basic logging configuration for local development. Copy from `logconfig.example.yaml`:

- Console output only
- INFO level logging  
- OTLP disabled by default
- Uses shared formatters from core templates

**When to use:** Normal local development when you just want console logs.

### logconfig.otlp.yaml (OTLP Enabled)

Enhanced logging configuration with OTLP enabled. Copy from `logconfig.otlp.example.yaml`:

- Sends logs to OTLP collector
- Both console and OTLP output
- Configurable OTLP endpoints

**When to use:** Testing observability features or integrating with monitoring systems.

**Key customizations:**
```yaml
# Update OTLP endpoints in the handlers
handlers:
  otlp:
    exporter:
      endpoint: http://your-otlp-collector:4317  # Customize this
      insecure: true  # Set to false for production endpoints
```

## Template System

The local configurations use the shared template system from `jobmon_core/src/jobmon/core/config/templates/`. This requires the OpenTelemetry packages to be available (even when OTLP is disabled) because the templates define OTLP formatters.

**Why OTLP packages are included:**
- The `!template formatters` directive loads all shared formatters, including OTLP ones
- This ensures consistency between local development and production configurations
- OTLP functionality is still **disabled** by default in the basic logging config

## Common Customization Scenarios

### 1. Local Database Setup

Update `jobmonconfig.local.yaml`:
```yaml
db:
  sqlalchemy_database_uri: "mysql+mysqldb://root:password@localhost:3306/jobmon_dev"
  sqlalchemy_connect_args:
    ssl_mode: "DISABLED"  # Disable SSL for local database
```

### 2. Different Frontend Port

Update `jobmonconfig.local.yaml`:
```yaml
cors:
  allowed_origins: "http://localhost:3001"  # Your frontend port
```

### 3. Enable OTLP for Testing

Update `jobmonconfig.local.yaml`:
```yaml
logging:
  server_logconfig_file: /app/config/logconfig.otlp.yaml
  client_logconfig_file: /app/config/logconfig.otlp.yaml
```

Then customize the OTLP endpoint in `logconfig.otlp.yaml`:
```yaml
handlers:
  otlp:
    exporter:
      endpoint: http://your-local-collector:4317
```

### 4. Production-like Setup

For testing against production services, update endpoints in your configs:
```yaml
# In logconfig.otlp.yaml
handlers:
  otlp:
    exporter:
      endpoint: https://otelcol.aks.your.domain.here443
      insecure: false
```

## Security Notes

- **Never commit your local config files to git** - they contain sensitive information
- The example files are safe to commit as they contain placeholder values
- Use environment variables for sensitive values when possible
- The `.gitignore` should exclude your local config files

## Troubleshooting

**Config file not found:**
- Ensure you've copied the example files and named them correctly
- Check that docker-compose.yml is mounting the `dev/config` directory

**Database connection issues:**
- Verify your database credentials in `jobmonconfig.local.yaml`
- Check that your database server is running and accessible

**OTLP connection issues:**
- Verify the OTLP collector endpoint in `logconfig.otlp.yaml`
- Check that the collector is running and reachable
- Consider using `insecure: true` for local testing 