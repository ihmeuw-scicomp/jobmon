from jobmon.server.web.api import get_app

# Note: With uvicorn --factory mode, get_app() is called directly by uvicorn
# per worker after forking. No need for module-level app creation.
