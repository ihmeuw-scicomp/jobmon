FROM python:3.13

# Set an environment variable for where your app code will live
ENV APP_HOME=/app
WORKDIR $APP_HOME

# Install uv
RUN python -m pip install --upgrade pip --no-cache-dir && \
    pip install --no-cache-dir uv

# Configure Artifactory for UV
ENV UV_EXTRA_INDEX_URL=https://artifactory.ihme.washington.edu/artifactory/api/pypi/pypi-shared/simple

# Set build-time arguments for extras
ARG EXTRAS=""
ARG EDITABLE=false

# Define valid extras for jobmon_core and jobmon_server (used by the script)
# These are now passed as environment variables to the script
ENV VALID_CORE_EXTRAS="otlp"
# Add other DB extras here, e.g., "postgres"
ENV VALID_SERVER_EXTRAS="otlp mysql"

# Copy source code
# Copies everything, including the new script from jobmon_server/
COPY ./ ./

# Install dependencies using the script
# Pass Docker ARGs as ENV VARs to the script
RUN EXTRAS_ARG="${EXTRAS}" \
    EDITABLE_ARG="${EDITABLE}" \
    APP_HOME="${APP_HOME}" \
    VALID_CORE_EXTRAS="${VALID_CORE_EXTRAS}" \
    VALID_SERVER_EXTRAS="${VALID_SERVER_EXTRAS}" \
    bash $APP_HOME/jobmon_server/docker_install_deps.sh

# Download NLTK data
RUN python -m nltk.downloader popular punkt_tab

# Ensure the main.py used by CMD is correctly pathed
COPY jobmon_server/main.py $APP_HOME/main.py
CMD ["fastapi", "run", "$APP_HOME/main.py", "--port", "8000"]
