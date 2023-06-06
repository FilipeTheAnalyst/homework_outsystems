with

fct_opportunity as (

    select * from {{ ref('fct_opportunity') }}

),

dim_account as (
    select * from {{ ref('dim_account') }}
),

dim_date as (
    select * from {{ ref('dim_date') }}
),

dim_location as (
    select * from {{ ref('dim_location') }}
),

dim_opportunity_detail as (
    select * from {{ ref('dim_opportunity_detail') }}
),


opportunity as 
(
    select
    fct_opportunity.opportunity_id,
    CASE    
        WHEN dim_account.company_size is null THEN 'Undefined'
        ELSE dim_account.company_size
    END AS company_size,
    CASE    
        WHEN dim_account.company_industry is null THEN 'Undefined'
        ELSE dim_account.company_industry
    END AS company_industry,
    CASE    
        WHEN dim_account.company_type is null THEN 'Undefined'
        ELSE dim_account.company_type
    END AS company_type,
    CASE    
        WHEN dim_account.number_of_employees is null THEN -1
        ELSE dim_account.number_of_employees
    END AS number_of_employees,
    CASE    
        WHEN dim_account.is_no_of_employees_valid is null THEN FALSE
        ELSE dim_account.is_no_of_employees_valid
    END AS is_no_of_employees_valid,
    CASE    
        WHEN dim_account.is_deleted is null THEN TRUE
        ELSE dim_account.is_deleted
    END AS company_is_deleted,

    dim_location.country,
    dim_location.region,
    dim_location.sales_territory,
    dim_opportunity_detail.is_deleted as opportunity_is_deleted,
    dim_opportunity_detail.stage_name,
    dim_opportunity_detail.stage_conclusion_pctg,
    dim_opportunity_detail.lead_source,
    dim_opportunity_detail.is_closed as opportunity_is_closed,
    dim_opportunity_detail.is_won as opportunity_is_won,
    dim_opportunity_detail.is_new_logo as opportunity_is_new_logo,
    dim_opportunity_detail.customer_type,
    dim_date.date_day as closed_date,
    dim_date.year as closed_year,
    dim_date.year_month as closed_year_month,
    dim_date.month as closed_month,
    dim_date.is_weekend as closed_is_weekend
    
    from fct_opportunity

    left join dim_account
        on fct_opportunity.account_id = dim_account.account_id

    left join dim_date 
        on fct_opportunity.closed_date = dim_date.date_id

    left join dim_location
        on fct_opportunity.location_id = dim_location.location_id

    left join dim_opportunity_detail
        on fct_opportunity.opportunity_detail_id = dim_opportunity_detail.opportunity_detail_id

)

select * from opportunity