

with raw_products as (
    select
        substring(md5(product || series) for 16) as product_id,
        product::text as product_name,
        series::text as product_series,
        sales_price::float as suggested_retail_price
    from 
        {{ source('crm_sales_data', 'products') }}
)

select * from raw_products