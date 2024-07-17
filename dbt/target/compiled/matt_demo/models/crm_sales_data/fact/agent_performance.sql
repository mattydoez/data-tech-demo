

with agent_performance as (
    select * 
    from "company_dw"."dev_crm_sales_dbt"."crm_stg_sales_pipelines"

)

select * from agent_performance