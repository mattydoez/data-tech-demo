sudo docker run --name test_postgres \
  --network airflow_test_network -d \
  -p 5432:5432 \
  -e POSTGRES_DB=company_db \
  -e POSTGRES_USER=ecomm_company \
  -e POSTGRES_PASSWORD=ecomm_company \
  -v $(pwd)/init.sql:/docker-entrypoint-initdb.d/init.sql \
  postgres:latest
