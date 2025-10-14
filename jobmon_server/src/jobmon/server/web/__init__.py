"""Web API subpackage."""

# Note: Logging configuration is now handled in get_app() to ensure it runs
# after uvicorn workers are forked, preventing duplicate log emissions.
