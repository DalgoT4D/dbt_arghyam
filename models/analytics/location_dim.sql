{{ config(materialized='table') }}

SELECT
    location_id AS location_sk,
    district_name AS district,
    block_name AS block,
    gp_name AS gram_panchayat,
    ward_name AS ward,
    CONCAT(block_name, ' ', ward_name) AS block_level,
    CURRENT_TIMESTAMP AS create_db_timestamp,
    '{{ invocation_id }}' AS create_audit_id,
    CURRENT_TIMESTAMP AS last_updated_timestamp,
    '{{ invocation_id }}' AS update_audit_id 
FROM
    {{ ref ('location_cdc') }} 
