sudo docker run --name test_airflow \
  --network airflow_test_network \
  -e AIRFLOW_HOME=/usr/local/airflow \
  -e LOAD_EX=n \
  -e EXECUTOR=Local \
  -e AIRFLOW__CORE__FERNET_KEY=GmhP3ADRHscUZ2z_ohwMOmXlu5jFSI5IQRG0s-KrV_Y= \
  -e AIRFLOW_CONN_COMPANY_DB=postgresql+psycopg2://ecomm_company:ecomm_company@test_postgres:5432/company_db \
  -v ./dags:/usr/local/airflow/dags \
  -v ./airflow.cfg:/usr/local/airflow/airflow.cfg \
  -v ~/home/matty/projects/portfolio_project/dbt-airflow-supserset/generator:/usr/local/airflow/generator \
  -p 8080:8080 \
  test_airflow webserver

sudo docker exec -it test_airflow airflow db init
sudo docker exec -it test_airflow airflow db upgrade

sudo docker exec -it test_airflow airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin_password

sudo docker exec -d test_airflow airflow scheduler