# Modern Data Tech Stack with Docker

This project demonstrates how to build a local data tech stack using Docker. The stack includes PostgreSQL for databases, Airflow for orchestrating workflows, dbt for data transformations, and Superset for data visualization. Each component is containerized for ease of setup and management.

## Prerequisites

- Docker
- Docker Compose

## Getting Started

### Environment Variables

Create a `.env` file in the project root to set up the required environment variables:

```
POSTGRES_USER=your_postgres_user
POSTGRES_PASSWORD=your_postgres_password
SUPERSET_ADMIN=your_superset_admin
SUPERSET_PASSWORD=your_superset_password
SUPERSET_SECRET_KEY=your_superset_secret_key
```

# Project Structure
```
├── README.MD
├── airflow
│   ├── Dockerfile
│   ├── airflow.cfg
│   ├── dags
│   ├── entrypoint.sh
│   ├── requirements.txt
│   ├── scripts
│   └── webserver_config.py
├── data
│   ├── crm_sales_data
├── dbt
│   ├── analyses
│   ├── dbt_packages
│   ├── dbt_project.yml
│   ├── macros
│   ├── models
│   ├── packages.yml
│   ├── profiles.yml
│   ├── requirements.txt
│   ├── seeds
│   ├── snapshots
│   ├── target
│   └── tests
├── docker-compose.yml
├── postgres_datawarehouse
│   └── postgresql.conf
└── superset
    ├── Dockerfile
    ├── init_superset.sh
    └── superset_config.py
```
### Setting Up Services

1. **Build and Start Services**

   ```bash
   docker-compose up --build

2. Access Services
	•	Airflow: http://localhost:8080
	•	Superset: http://localhost:8088

### Services Overview
#### PostgreSQL:
### Services Overview

- **PostgreSQL**:
  - **Airflow DB**: Used by Airflow for metadata.
  - **Company DW**: Data warehouse for storing and processing data.
  - **Superset DB**: Used by Superset for metadata and storing dashboard data.
  - **Exposed Ports**:
    - Airflow DB: `5433`
    - Company DW: `5435`
    - Superset DB: `5434`

- **Airflow**:
  - Orchestrates workflows.
  - Contains several services: `airflow-scheduler`, `airflow-webserver`, `airflow-worker`, `airflow-init`.
  - Airflow webserver is accessible at port `8080`.
  - **Resources**:
    - [Airflow Documentation](https://airflow.apache.org/docs/)

- **Redis**:
  - Message broker for Airflow's CeleryExecutor.
  - **Exposed Port**: `6379`
  - **Resources**:
    - [Redis Documentation](https://redis.io/documentation)

- **dbt**:
  - Transforms data in the data warehouse.
  - Run dbt commands within the `dbt-serve` service.
  - **Resources**:
    - [dbt Documentation](https://docs.getdbt.com/docs/introduction)

- **Superset**:
  - Visualizes data stored in the data warehouse.
  - Connects to the `superset_db` PostgreSQL instance.
  - Superset is accessible at port `8088`.
  - **Resources**:
    - [Superset Documentation](https://superset.apache.org/docs/)

### Data Ingestion and Transformation

- **Sample Data**: Located in the `data` folder.
  - `accounts.csv`
  - `sales_pipeline.csv`
  - `sales_teams.csv`
- **Airflow Service**: Ingests the sample data and runs dbt models located in the `dbt` folder.
- **DBT Models**: Transform the ingested data in the data warehouse.

### Conclusion

By following these steps, you can set up a robust data tech stack locally using Docker. This setup includes PostgreSQL for databases, Airflow for workflow orchestration, dbt for data transformation, and Superset for data visualization. Each service is containerized for easy management and deployment. Use the provided links to explore more about each service and enhance your data stack further.
