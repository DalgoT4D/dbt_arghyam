{{ 
    config(
        materialized='table'
    ) 
}}


WITH 
    cte_get_max_pk AS (
        -- Getting the last PK in target table
        SELECT COALESCE(MAX(location_dim_id), 0 ) AS max_sk
        FROM {{ this }}
    )

SELECT
    ((SELECT max_sk FROM cte_get_max_pk) + (ROW_NUMBER() OVER (ORDER BY (SELECT 1)))) AS location_dim_id
    , sub.ward_name AS ward
    , sub.block_name AS block
    , sub.district_name AS district
    , sub.gp_name AS gram_panchayat
    , CURRENT_TIMESTAMP AS create_db_timestamp
    , '{{ invocation_id }}' AS create_audit_id
    , CURRENT_TIMESTAMP AS last_updated_timestamp
    , '{{ invocation_id }}' AS update_audit_id 
    , 1 AS is_active
    /*CASE -- manually updated with 1 (True) or 0 (False) when a ward becomes inactive
        WHEN tgt.is_active = 0 THEN 0 
        ELSE 1*/
FROM
    {{ ref ('location_intermediate') }} AS sub
-- LEFT JOIN {{ source ('analytics', 'location_dim') }} AS tgt ON -- need NK