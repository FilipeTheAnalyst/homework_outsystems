with

opportunity_detail as (

    select * from {{ ref('stg_sales__opportunity') }}

),

dim_opportunity_detail as 
(
    select distinct
        {{ dbt_utils.generate_surrogate_key(['stage_name', 'stage_conclusion_pctg', 'lead_source', 'customer_type',
        'IS_CLOSED', 'is_won', 'is_new_logo', 'is_deleted']) }} as opportunity_detail_id,
        stage_name,
        stage_conclusion_pctg,
        lead_source,
        customer_type,
        IS_CLOSED,
        is_won,
        is_new_logo,
        is_deleted

    from opportunity_detail
)

select * from dim_opportunity_detail