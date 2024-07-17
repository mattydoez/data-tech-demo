select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    with invalid_timestamps as (
        select
            close_date
        from
            "company_dw"."dev"."crm_stg_sales_pipelines"
        where
            close_date is not null
            and (
                to_timestamp(close_date, 'YYYY-MM-DD HH24:MI:SS') is null
                or to_timestamp(close_date, 'YYYY-MM-DD HH24:MI:SS') = 'epoch'
            )
    )

    select * from invalid_timestamps

      
    ) dbt_internal_test