{{
config(
materialized = 'table'
)
}}

select * 
from {{ ref('crm_int_kpi') }}