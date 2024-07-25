{{
config(
materialized = 'table'
)
}}


WITH quarterly_revenue AS (
    SELECT
        date_trunc('quarter', ts_close_date) AS quarter,
        SUM(revenue) AS total_revenue
    FROM
        {{ ref('crm_int_kpi') }}
    GROUP BY
        date_trunc('quarter', ts_close_date)
    ORDER BY
        quarter
),
quarterly_growth AS (
    SELECT
        quarter,
        total_revenue,
        LAG(total_revenue) OVER (ORDER BY quarter) AS previous_quarter_revenue,
        CASE
            WHEN LAG(total_revenue) OVER (ORDER BY quarter) IS NULL THEN NULL
            ELSE (total_revenue - LAG(total_revenue) OVER (ORDER BY quarter)) / LAG(total_revenue) OVER (ORDER BY quarter) * 100
        END AS quarter_over_quarter_growth
    FROM
        quarterly_revenue
)
SELECT
    quarter,
    total_revenue,
    previous_quarter_revenue,
    quarter_over_quarter_growth
FROM
    quarterly_growth