select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select sales_agent
from "company_dw"."dev_crm_sales_dbt_crm_sales_dbt"."crm_stg_sales_teams"
where sales_agent is null



      
    ) dbt_internal_test