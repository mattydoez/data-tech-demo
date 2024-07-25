

with raw_sales_pipeline as (
    select
        opportunity_id::text,
        case when sales_agent = 'NaN' then 'Unknown' else sales_agent::text end as sales_agent,
        case when product = 'NaN' then 'Unknown' else product::text end as product_name,
        case when account = 'NaN' then 'Unknown' else account::text end as company_name,
        case when deal_stage = 'NaN' then 'Unknown' else deal_stage::text end as deal_stage,
        case when engage_date = 'NaN' then null else engage_date::timestamp end as ts_engage_date,
        case when close_date = 'NaN' then null else close_date::timestamp end as ts_close_date,
        case when engage_date = 'NaN' then null
             when((close_date = 'NaN' or close_date is null) and (engage_date != 'NaN' and engage_date is not null))
                then age(current_date, engage_date::timestamp)
                else age(close_date::timestamp, engage_date::timestamp) end as deal_age,
        case when close_value = 'NaN' then 0 else close_value::float end as revenue
    from 
        {{ source('crm_sales_data', 'sales_pipeline') }}
)

select * from raw_sales_pipeline