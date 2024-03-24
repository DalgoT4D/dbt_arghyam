INSERT INTO dalgo.test.location_dim(
    location_dim_id, location_nk, ward, block, district, gram_panchayat, 
    create_db_timestamp, create_audit_id, last_updated_timestamp, update_audit_id)
SELECT
    (ROW_NUMBER() OVER (ORDER BY (SELECT 1))) AS location_dim_id
    , sub.location_id AS location_nk
    , sub.ward_name AS ward
    , sub.block_name AS block
    , sub.district_name AS district
    , sub.gp_name AS gram_panchayat
    , CURRENT_TIMESTAMP AS create_db_timestamp
    , '{{ invocation_id }}' AS create_audit_id
    , CURRENT_TIMESTAMP AS last_updated_timestamp
    , '{{ invocation_id }}' AS update_audit_id 
FROM
    {{ ref ('location_cdc') }} AS sub