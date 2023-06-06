with

location as (

    select * from {{ ref('stg_sales__opportunity') }}

),

dim_location as 
(
    select distinct
        {{ dbt_utils.generate_surrogate_key(['country', 'region', 'sales_territory']) }} as location_id,
        country,
        region,
        sales_territory

    from location
)

select * from dim_location