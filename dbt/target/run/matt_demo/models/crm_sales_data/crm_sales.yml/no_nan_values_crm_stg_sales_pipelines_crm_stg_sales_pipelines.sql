select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    
    

    
    
        
            
            
        
    
        
            
            
        
    
        
            
            
        
    
        
            
            
        
    
        
            
            
        
    
        
    
        
    
        
    
        
    

    
    
        
        SELECT * FROM "company_dw"."dev_crm_sales_dbt"."crm_stg_sales_pipelines" WHERE opportunity_id = 'NaN' UNION ALL SELECT * FROM "company_dw"."dev_crm_sales_dbt"."crm_stg_sales_pipelines" WHERE sales_agent = 'NaN' UNION ALL SELECT * FROM "company_dw"."dev_crm_sales_dbt"."crm_stg_sales_pipelines" WHERE product_name = 'NaN' UNION ALL SELECT * FROM "company_dw"."dev_crm_sales_dbt"."crm_stg_sales_pipelines" WHERE company_name = 'NaN' UNION ALL SELECT * FROM "company_dw"."dev_crm_sales_dbt"."crm_stg_sales_pipelines" WHERE deal_stage = 'NaN'
    

      
    ) dbt_internal_test