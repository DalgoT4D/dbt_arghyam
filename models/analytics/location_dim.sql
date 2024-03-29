{{ 
    config(
        materialized='table'
    ) 
}}


SELECT
    sub.location_id AS location_sk
    , sub.district_name AS district
    , sub.block_name AS block
    , sub.gp_name AS gram_panchayat
    , sub.ward_name AS ward
    , CASE 
        WHEN sub.op_type='C' THEN CURRENT_TIMESTAMP
        ELSE tgt.create_db_timestamp
    END AS create_db_timestamp
    , CASE 
        WHEN sub.op_type='C' THEN '{{ invocation_id }}'
        ELSE tgt.create_audit_id
    END AS create_audit_id
    , CASE 
        WHEN sub.op_type='U' THEN CURRENT_TIMESTAMP
        ELSE tgt.last_updated_timestamp
    END AS last_updated_timestamp
    , CASE 
        WHEN sub.op_type='U' THEN '{{ invocation_id }}'
        ELSE tgt.update_audit_id 
    END AS update_audit_id 
    /*CASE -- manually updated with 1 (True) or 0 (False) when a ward becomes inactive || using voided column
        WHEN tgt.is_active = 0 THEN 0 
        ELSE 1*/
FROM
    {{ ref ('location_cdc') }} AS sub
LEFT JOIN {{ source ('analytics', 'location_dim') }} AS tgt ON sub.location_id = tgt.location_sk
