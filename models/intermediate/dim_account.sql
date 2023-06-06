with

account as (

    select * from {{ ref('stg_sales__account') }}

),

dim_account as 
(
    account_id,
    is_deleted,
    company_size,
    created_date,
    company_industry,
    number_of_employees,
    is_no_of_employees_valid,
    company_type,
    is_new_logo
)

select * from dim_account