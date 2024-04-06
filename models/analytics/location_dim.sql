{{ 
    config(
        materialized='incremental',
        unique_key = 'location_id',
        merge_update_columns = ['location_sk', 'create_db_timestamp', 
                            'create_audit_id']
    ) 
}}


SELECT
    sub.location_id AS location_sk
    , sub.district_name AS district
    , sub.block_name AS block
    , sub.gp_name AS gram_panchayat
    , sub.ward_name AS ward
    , CONCAT(sub.block_name, ' ', sub.ward_name) AS block_level
    , CURRENT_TIMESTAMP AS create_db_timestamp
    , '{{ invocation_id }}' AS create_audit_id
    , CURRENT_TIMESTAMP AS last_updated_timestamp
    , '{{ invocation_id }}' AS update_audit_id 
    /*CASE -- manually updated with 1 (True) or 0 (False) when a ward becomes inactive || using voided column
        WHEN tgt.is_active = 0 THEN 0 
        ELSE 1*/
FROM
    {{ ref ('location_cdc') }} AS sub
-- LEFT JOIN {{ source ('analytics', 'location_dim') }} AS tgt ON sub.location_id = tgt.location_sk
