FROM python:3.12

# Set build-time arguments
ARG EXTRAS=""
ARG EDITABLE=false

# Define valid extras for jobmon_core and jobmon_server
ENV VALID_CORE_EXTRAS="otlp"
ENV VALID_SERVER_EXTRAS="otlp mysql"

# Copy the entire jobmon directory
# Consider using .dockerignore to exclude files not needed for the build
COPY ./ /app/jobmon
RUN cp /app/jobmon/jobmon_server/main.py /app/main.py

# Generate a requirements.txt file and install dependencies
RUN bash -c ' \
    CORE_EXTRAS=""; \
    SERVER_EXTRAS=""; \
    for EXTRA in ${EXTRAS//,/ }; do \
        if echo "$VALID_CORE_EXTRAS" | grep -qw "$EXTRA"; then \
            CORE_EXTRAS="${CORE_EXTRAS},$EXTRA"; \
        fi; \
        if echo "$VALID_SERVER_EXTRAS" | grep -qw "$EXTRA"; then \
            SERVER_EXTRAS="${SERVER_EXTRAS},$EXTRA"; \
        fi; \
    done; \
    if [ -n "$CORE_EXTRAS" ]; then CORE_EXTRAS="[${CORE_EXTRAS#,}]"; fi; \
    if [ -n "$SERVER_EXTRAS" ]; then SERVER_EXTRAS="[${SERVER_EXTRAS#,}]"; fi; \
    INSTALL_MODE=""; \
    if [ "$EDITABLE" = "true" ]; then INSTALL_MODE="-e"; fi; \
    echo "$INSTALL_MODE /app/jobmon/jobmon_core${CORE_EXTRAS}" > requirements.txt; \
    echo "$INSTALL_MODE /app/jobmon/jobmon_server${SERVER_EXTRAS}" >> requirements.txt; \
    pip install -r requirements.txt \
'

# Download NLTK data
RUN python -m nltk.downloader popular punkt_tab

# Set the command to run the server
CMD ["fastapi", "run", "/app/main.py", "--port", "80"]
