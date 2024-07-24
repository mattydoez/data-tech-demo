
  create view "company_dw"."dev_crm_sales_dbt_crm_sales_dbt"."crm_stg_sales_teams__dbt_tmp"
    
    
  as (
    

with raw_sales_teams as (
    select
        sales_agent::text,
        manager::text,
        regional_office::text,
        manager::text || cast(' | ' as text) || regional_office::text as team_name
    from 
        "company_dw"."crm_sales_data"."sales_teams"
)

select * from raw_sales_teams
  );