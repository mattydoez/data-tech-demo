

with 
    pipelines as (
        select * 
        from {{ ref('crm_stg_sales_pipelines') }}
    ),
    accounts as (
        select 
            account_name,
            industry, 
            num_employees, 
            annual_revenue_mm
        from {{ ref('crm_stg_accounts') }}
    ), 
    products as (
        select 
             product_name,
             suggested_retail_price
        from {{ ref('crm_stg_products') }}
    ),
    teams as (
        select 
            sales_agent,
            manager,
            team_name,
            regional_office 
        from {{ ref('crm_stg_sales_teams') }}
    )
select 
    p.*,
    industry,
    num_employees,
    annual_revenue_mm,
    suggested_retail_price,
    manager,
    team_name,
    regional_office
from pipelines p
left join accounts a on p.company_name = a.account_name 
left join products pr on p.product_name = pr.product_name 
left join teams t on p.sales_agent = t.sales_agent