# generator/Dockerfile

FROM python:3.9-slim

WORKDIR /home/generator

# Install PostgreSQL client libraries
RUN apt-get update && apt-get install -y \
    libpq-dev \
    build-essential

RUN mkdir csv
RUN mkdir xml

COPY generator .

RUN pip install -r requirements.txt