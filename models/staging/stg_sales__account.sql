with

account as (

    select * from {{ source('sales', 'account') }}

),

final as (

    select
        id as account_id,
        is_deleted as is_deleted,
        type as company_type,
        CASE
            WHEN industry is null THEN 'Unknown'
            ELSE industry
        END as company_industry,
        CASE
            WHEN number_of_employees is null THEN -1
            WHEN number_of_employees = 0 THEN -1
            ELSE number_of_employees
        END as number_of_employees,
        CASE 
            WHEN number_of_employees BETWEEN -1 AND 0 THEN FALSE
            WHEN number_of_employees is null THEN FALSE
            ELSE TRUE
        END AS is_no_of_employees_valid,
        DATE(created_date) as created_date,
        CASE
            WHEN number_of_employees BETWEEN 1 AND 200 THEN 'SMB'
            WHEN number_of_employees BETWEEN 201 AND 4500 THEN 'Commercial'
            WHEN number_of_employees > 4500 THEN 'Enterprise'
            ELSE 'Undefined'
        END as company_size

    from account

    UNION

    select

    'N/A' as account_id,
    TRUE as is_deleted,
    'Unknown' as company_type,
    'Unknown' as company_industry,
    -1 as number_of_employees,
    FALSE as is_no_of_employees_valid,
    date(current_timestamp()) as created_date,
    'Undefined' as company_size

)

select * from final