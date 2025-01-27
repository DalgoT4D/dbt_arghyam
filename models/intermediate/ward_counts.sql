{{ config(materialized='table') }}


WITH total_wards_cte AS (
    SELECT COUNT(DISTINCT "Catchement( Dist, Block, GP, Ward)") AS total_wards
    FROM 
        prod_intermediate_seeds.ward_master_list
),

received_wards_cte AS (
    SELECT COUNT(DISTINCT tenantid) AS received_wards
    FROM 
        {{ ref('transformed_tenantid') }}
)

SELECT 
    total_wards_cte.total_wards,
    received_wards_cte.received_wards
FROM 
    total_wards_cte, received_wards_cte
