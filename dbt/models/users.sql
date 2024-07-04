{{ config(
    materialized='view'
) }}

SELECT
    *
FROM
    {{ source('company_db', 'users') }}