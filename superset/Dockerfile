FROM apache/superset:latest

# Install necessary dependencies
USER root
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev libffi-dev \
    libxml2-dev libxslt1-dev \
    postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy the initialization script
COPY init_superset.sh /app/init_superset.sh

# Copy the Superset config file
COPY superset_config.py /app/pythonpath/superset_config.py

# Copy asset files
COPY assets /app/assets

# Ensure the script is executable
RUN chmod +x /app/init_superset.sh


# Switch back to the superset user
USER superset

# Expose the port that superset runs on
EXPOSE 8088

# Command to run the superset server
CMD ["/app/init_superset.sh"]