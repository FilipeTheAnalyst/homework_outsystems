with

date as (

    select * from {{ ref('stg_sales__opportunity') }}

),

dim_date as 
(
    {{ dbt_utils.date_spine(
        datepart = "day",
        start_date = "to_date('01-01-2017', 'dd-mm-yyyy')",
        end_date = "to_date('01-01-2023', 'dd-mm-yyyy')"
    ) }}
),

final as 
(
    select 
    TO_NUMBER(TO_CHAR(date_day, 'YYYYMMDD')) as date_id,
    date_day,
    DATE_PART(YEAR, date_day) AS year,
    TO_CHAR(date_day, 'YYYY-MM') AS year_month,
    DATE_PART(MONTH, date_day) AS month,
    CASE 
        WHEN DAYOFWEEK(date_day) IN (1, 7) THEN TRUE 
        ELSE FALSE 
    END AS is_weekend
    from dim_date
)

select * from final