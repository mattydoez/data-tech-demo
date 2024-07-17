select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    sales_agent as unique_field,
    count(*) as n_records

from "company_dw"."dev_crm_sales_dbt"."crm_stg_sales_teams"
where sales_agent is not null
group by sales_agent
having count(*) > 1



      
    ) dbt_internal_test