-- users.sql
{{ config(
    materialized='table'
) }}

SELECT
    *
FROM
    {{ source('company_db', 'users') }}