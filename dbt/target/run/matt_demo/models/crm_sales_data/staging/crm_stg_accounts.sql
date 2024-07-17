
  create view "company_dw"."dev_crm_sales_dbt"."crm_stg_accounts__dbt_tmp"
    
    
  as (
    

with raw_accounts as (
    select
        substring(md5(account || sector || year_established || office_location) for 16) as account_id,
        account::text as account_name,
        sector::text as industry,
        year_established::int,
        revenue::float as annual_revenue_mm,
        employees::int as num_employees,
        office_location::text as headquarters,
        subsidiary_of::text as parent_company
    from 
        "company_dw"."crm_sales_data"."accounts"
)

select * from raw_accounts
  );