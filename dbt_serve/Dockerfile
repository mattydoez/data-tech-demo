# Use the official Python image as a base
FROM python:3.9-slim

# Set the working directory
WORKDIR /usr/app/dbt

# Install dbt-core and dbt-postgres
RUN pip install --upgrade pip && \
    pip install dbt-core dbt-postgres

# Expose the port that dbt serve will use
EXPOSE 8001

# Command to run dbt serve
CMD ["dbt", "docs", "serve", "--port", "8001"]