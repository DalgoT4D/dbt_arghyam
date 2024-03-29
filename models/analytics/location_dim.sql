{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='location_sk',
    ) 
}}


SELECT
    CASE 
        WHEN sub.op_type = 'C' THEN sub.location_id 
        ELSE tgt.location_sk
    END AS location_sk
    , CASE 
        WHEN sub.op_type IN ('C', 'U') THEN sub.district_name
        ELSE tgt.district
    END AS district
    , CASE 
        WHEN sub.op_type IN ('C', 'U') THEN sub.block_name 
        ELSE tgt.block 
    END AS block
    , CASE 
        WHEN sub.op_type IN ('C', 'U') THEN sub.gp_name
        ELSE tgt.gram_panchayat
    END AS gram_panchayat
    , CASE 
        WHEN sub.op_type IN ('C', 'U') THEN sub.ward_name 
        ELSE tgt.ward
    END AS ward
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

{% if is_incremental() %}
    WHERE sub.src_last_modified_at >= (SELECT MAX(last_updated_timestamp) FROM {{ this }})
{% endif %}

