
  
    

  create  table "company_dw"."dev_crm_sales_dbt"."kpi__dbt_tmp"
  
  
    as
  
  (
    

select * 
from "company_dw"."dev_crm_sales_dbt"."crm_int_kpi"
  );
  