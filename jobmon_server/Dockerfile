FROM python:3.12

# for now, we will copy the entire jobmon directory.
# Consider using .dockerignore to exclude files not needed for the build.
COPY ./ /app/jobmon
RUN cp /app/jobmon/jobmon_server/main.py /app/main.py

# Install your local packages
RUN pip install "/app/jobmon/jobmon_core[otlp]" && \
    pip install -e "/app/jobmon/jobmon_server[otlp]"

RUN python -m nltk.downloader popular punkt_tab

CMD ["fastapi", "run", "/app/main.py", "--port", "80", "--reload"]
