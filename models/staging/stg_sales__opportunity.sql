{{ config(
    materialized='view',
    pre_hook='UPDATE {{ this }} AS t1
SET t1.region_c_c = t2.region_c_c
FROM (
    SELECT country_c, MAX(region_c_c) AS region_c_c
    FROM {{ this }}
    WHERE region_c_c IS NOT NULL
    GROUP BY country_c
) AS t2
WHERE t1.country_c = t2.country_c
AND t1.region_c_c IS NULL'
) }}

with

opportunity as (

    select * from {{ source('sales', 'opportunity') }}

),

final as (

    select
        id as opportunity_id,
        is_deleted as is_deleted,
        CASE
            WHEN account_id is null THEN 'N/A'
            ELSE account_id
        END as account_id,
        stage_name,
        CASE
            WHEN stage_name = 'Closed Lost' THEN '100%'
            ELSE SUBSTRING(stage_name,CHARINDEX('(', stage_name) + 1,CHARINDEX(')', stage_name) - CHARINDEX('(', stage_name) - 1)
        END AS stage_conclusion_pctg,
        close_date,
        CASE    
            WHEN lead_source is null THEN 'Unknown'
            ELSE lead_source
        END AS lead_source,
        is_closed,
        is_won,
        DATE(created_date) as created_date,
        CASE
            WHEN country_c is null THEN 'Undefined'
            ELSE country_c
        END AS country,
        CASE
            WHEN region_c_c is null THEN 'Undefined'
            ELSE region_c_c
        END AS region,
        sales_territory_c as sales_territory,
        IFF(is_new_logo_c='Yes', True, False) as is_new_logo

        
    from opportunity

)

select * from final