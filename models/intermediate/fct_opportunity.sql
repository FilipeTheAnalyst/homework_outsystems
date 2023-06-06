with

opportunity as (

    select * from {{ ref('stg_sales__opportunity') }}

),

account as (
    select * from {{ ref('dim_account') }}
),

dim_date as (
    select * from {{ ref('dim_date') }}
),

location as (
    select * from {{ ref('dim_location') }}
),

opportunity_detail as (
    select * from {{ ref('dim_opportunity_detail') }}
),


fct_opportunity as 
(
    select
    opportunity.opportunity_id,
    CASE
        WHEN account.account_id is null THEN 'Unknown'
        ELSE account.account_id
    END AS account_id,
    TO_NUMBER(TO_CHAR(opportunity.created_date, 'YYYYMMDD')) as created_date,
    TO_NUMBER(TO_CHAR(opportunity.close_date, 'YYYYMMDD')) as closed_date,
    location.location_id,
    opportunity_detail.opportunity_detail_id
    
    from opportunity

    left join account
        on opportunity.account_id = account.account_id

    left join dim_date 
        on TO_NUMBER(TO_CHAR(opportunity.created_date, 'YYYYMMDD')) = dim_date.date_id

    left join location
        on opportunity.country = location.country
        and opportunity.region = location.region
        and opportunity.sales_territory = location.sales_territory

    left join opportunity_detail
        on opportunity.is_deleted = opportunity_detail.is_deleted
        and opportunity.stage_name = opportunity_detail.stage_name
        and opportunity.stage_conclusion_pctg = opportunity_detail.stage_conclusion_pctg
        and opportunity.lead_source = opportunity_detail.lead_source
        and opportunity.is_closed = opportunity_detail.is_closed
        and opportunity.is_won = opportunity_detail.is_won
        and opportunity.is_new_logo = opportunity_detail.is_new_logo
        and opportunity.customer_type = opportunity_detail.customer_type
)

select * from fct_opportunity