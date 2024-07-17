{{
config(
materialized = 'table',
schema = 'crm_sales_dbt'
)
}}

select * 
from {{ ref('crm_int_kpi') }}