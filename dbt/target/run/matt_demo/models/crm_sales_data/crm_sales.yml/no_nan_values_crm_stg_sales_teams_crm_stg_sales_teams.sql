select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    
    

    
    
        
            
            
        
    
        
            
            
        
    
        
            
            
        
    
        
            
            
        
    

    
    
        
        SELECT * FROM "company_dw"."dev_crm_sales_dbt"."crm_stg_sales_teams" WHERE sales_agent = 'NaN' UNION ALL SELECT * FROM "company_dw"."dev_crm_sales_dbt"."crm_stg_sales_teams" WHERE manager = 'NaN' UNION ALL SELECT * FROM "company_dw"."dev_crm_sales_dbt"."crm_stg_sales_teams" WHERE regional_office = 'NaN' UNION ALL SELECT * FROM "company_dw"."dev_crm_sales_dbt"."crm_stg_sales_teams" WHERE team_name = 'NaN'
    

      
    ) dbt_internal_test