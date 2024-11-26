{% set invocation_id = 'some_value' %}

INSERT INTO dalgo.test.location_dim(
    location_sk, district, block, gram_panchayat, ward,
    create_db_timestamp, create_audit_id, last_updated_timestamp, update_audit_id
)
SELECT
    sub.location_id AS location_sk,
    sub.district_name AS district,
    sub.block_name AS block,
    sub.gp_name AS gram_panchayat,
    sub.ward_name AS ward,
    CURRENT_TIMESTAMP AS create_db_timestamp,
    '{{ invocation_id }}' AS create_audit_id,
    CURRENT_TIMESTAMP AS last_updated_timestamp,
    '{{ invocation_id }}' AS update_audit_id
FROM
    {{ ref('location_cdc') }} AS sub;
