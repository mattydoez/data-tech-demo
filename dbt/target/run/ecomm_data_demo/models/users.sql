
  
    

  create  table "company_dw"."public"."users__dbt_tmp"
  
  
    as
  
  (
    -- users.sql


SELECT
    *
FROM
    "company_db"."public"."users"
  );
  