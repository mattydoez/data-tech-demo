select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select opportunity_id
from "company_dw"."dev_crm_sales_dbt_crm_sales_dbt"."crm_stg_sales_pipelines"
where opportunity_id is null



      
    ) dbt_internal_test