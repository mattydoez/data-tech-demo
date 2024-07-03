#!/usr/bin/env bash

# If the first argument is one of the Airflow commands, use it
if [[ "$1" == "scheduler" || "$1" == "webserver" || "$1" == "initdb" || "$1" == "kerberos" || "$1" == "worker" || "$1" == "flower" || "$1" == "triggerer" || "$1" == "standalone" || "$1" == "sync-perm" || "$1" == "info" || "$1" == "connections" || "$1" == "dags" || "$1" == "db" || "$1" == "jobs" || "$1" == "pools" || "$1" == "providers" || "$1" == "roles" || "$1" == "rotate-fernet-key" || "$1" == "tasks" || "$1" == "users" || "$1" == "variables" || "$1" == "version" ]]; then
  exec airflow "$@"
else
  # Run the provided command
  exec "$@"
fi