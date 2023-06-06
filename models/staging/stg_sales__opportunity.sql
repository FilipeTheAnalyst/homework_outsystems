with

opportunity as (

    select * from {{ source('sales', 'opportunity') }}

),

cte AS (
  SELECT COUNTRY_C, MAX(REGION_C_C) AS REGION_C_C, MAX(SALES_TERRITORY_C) AS SALES_TERRITORY_C
  FROM opportunity
  WHERE REGION_C_C IS NOT NULL AND SALES_TERRITORY_C IS NOT NULL
  GROUP BY COUNTRY_C
),

final as 
(
SELECT
  t.ID AS opportunity_id,
  t.IS_DELETED,
  CASE
        WHEN t.account_id is null THEN 'N/A'
        ELSE t.account_id
  END as account_id,
  t.STAGE_NAME,
  CASE
            WHEN t.stage_name = 'Closed Lost' THEN '100%'
            ELSE SUBSTRING(t.stage_name,CHARINDEX('(', t.stage_name) + 1,CHARINDEX(')', t.stage_name) - CHARINDEX('(', t.stage_name) - 1)
    END AS stage_conclusion_pctg,
  t.CLOSE_DATE,
  CASE    
        WHEN t.lead_source is null THEN 'Unknown'
        ELSE t.lead_source
    END AS lead_source,
  t.IS_CLOSED,
  t.IS_WON,
  DATE(t.CREATED_DATE) AS created_date,
  CASE
      WHEN t.COUNTRY_C is null THEN 'Undefined'
      ELSE t.country_c
    END as country,
CASE
    WHEN COALESCE(t.REGION_C_C, cte.REGION_C_C) is null THEN 'Undefined'
    ELSE COALESCE(t.REGION_C_C, cte.REGION_C_C)
END AS region,
  t.TYPE_C AS customer_type,
  COALESCE(t.SALES_TERRITORY_C, cte.SALES_TERRITORY_C) AS sales_territory,
  IFF(t.is_new_logo_c='Yes', True, False) as is_new_logo
FROM opportunity AS t
LEFT JOIN cte ON t.COUNTRY_C = cte.COUNTRY_C
)


select * from final