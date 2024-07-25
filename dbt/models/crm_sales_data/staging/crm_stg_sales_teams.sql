

with raw_sales_teams as (
    select
        sales_agent::text,
        manager::text,
        regional_office::text,
        manager::text || cast(' | ' as text) || regional_office::text as team_name
    from 
        {{ source('crm_sales_data', 'sales_teams') }}
)

select * from raw_sales_teams