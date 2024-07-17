
  
    

  create  table "company_dw"."dev"."crm_performance__dbt_tmp"
  
  
    as
  
  (
    

with sales_performance as (
    select
        st.manager,
        st.team_name,
        st.sales_agent,
        sp.opportunity_id,
        sp.product_name,
        sp.company_name,
        sp.deal_stage,
        sp.engage_date,
        sp.close_date,
        sp.revenue,
        date_trunc('quarter', sp.close_date) as close_quarter
    from 
        "company_dw"."dev"."crm_stg_sales_pipelines" sp
    join 
        "company_dw"."dev"."crm_stg_sales_teams" st on sp.sales_agent = st.sales_agent
),

team_performance as (
    select
        manager,
        team_name,
        count(opportunity_id) as total_opportunities,
        sum(revenue) as total_value,
        avg(revenue) as average_value,
        array_agg(distinct sales_agent) as sales_agents
    from 
        sales_performance
    where 
        deal_stage = 'Won'
    group by 
        manager, team_name
),

agent_performance as (
    select
        sales_agent,
        manager,
        count(opportunity_id) as won_opportunities,
        sum(revenue) as total_value
    from 
        sales_performance
    where 
        deal_stage = 'Won'
    group by 
        sales_agent, manager
    order by 
        won_opportunities asc
),

quarterly_trends as (
    select
        close_quarter,
        count(opportunity_id) as total_opportunities,
        sum(revenue) as total_value,
        avg(revenue) as average_value
    from 
        sales_performance
    group by 
        close_quarter
    order by 
        close_quarter
),

product_win_rates as (
    select
        product_name,
        count(case when deal_stage = 'Won' then 1 end) as won_opportunities,
        count(case when deal_stage = 'Lost' then 1 end) as lost_opportunities,
        round(
            (count(case when deal_stage = 'Won' then 1 end) * 1.0 / 
            (count(case when deal_stage = 'Won' then 1 end) + count(case when deal_stage = 'Lost' then 1 end))) * 100, 2
        ) as win_rate
    from 
        sales_performance
    group by 
        product_name
)

select 
    tp.manager,
    tp.team_name,
    tp.total_opportunities,
    tp.total_value,
    tp.average_value,
    tp.sales_agents,
    ap.sales_agent,
    ap.won_opportunities,
    ap.total_value as agent_total_value,
    qt.close_quarter,
    qt.total_opportunities as quarter_total_opportunities,
    qt.total_value as quarter_total_value,
    qt.average_value as quarter_average_value,
    pw.product_name,
    pw.won_opportunities as product_won_opportunities,
    pw.lost_opportunities as product_lost_opportunities,
    pw.win_rate
from 
    team_performance tp
left join 
    agent_performance ap on tp.manager = ap.manager
left join 
    quarterly_trends qt on tp.manager is not null
left join 
    product_win_rates pw on tp.manager is not null
  );
  