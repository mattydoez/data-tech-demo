FROM apache/superset:latest

# Install necessary dependencies
USER root
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev libffi-dev \
    libxml2-dev libxslt1-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to the superset user
USER superset

# Expose the port that superset runs on
EXPOSE 8088

# Command to run the superset server
CMD ["gunicorn", "--bind", "0.0.0.0:8088", "--access-logfile", "-", "--error-logfile", "-", "--workers", "1", "--worker-class", "gthread", "--threads", "20", "--timeout", "60", "--limit-request-line", "0", "--limit-request-field_size", "0", "superset.app:create_app()"]