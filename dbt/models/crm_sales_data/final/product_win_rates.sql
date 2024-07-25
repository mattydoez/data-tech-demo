{{
config(
materialized = 'table'
)
}}

WITH win_counts AS (
    SELECT product_name AS product_name,
        sum(case
                when deal_stage = 'Won' then 1
                else 0
            end) AS won_deals,
        sum(case
                when (deal_stage != 'NaN'
                        and deal_stage is not null) then 1
                else 0
            end) AS all_deals
    FROM
    (SELECT *
    FROM {{ ref('crm_int_kpi') }} a
    LEFT JOIN dev_global_dim.dim_date b ON a.ts_close_date = b.date_actual) AS virtual_table
    GROUP BY product_name
    ORDER BY won_deals DESC )
select product_name,
        sum(won_deals) as won_deals,
        sum(all_deals) as all_deals,
    sum(won_deals) / sum(all_deals) as product_win_rate
from win_counts
group by product_name