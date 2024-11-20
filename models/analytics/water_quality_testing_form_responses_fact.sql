{{
    config(
        materialized='table',
        schema='analytics'
    )
}}

WITH 
wq_raw_data AS (
    SELECT 
        enc.id AS encounter_id,
        enc.subject_type,
        enc.username,
        sub.location,
        enc.observations,
        enc.audit,
        enc.meeting_date,
        brd.ward_name,
        brd.block_name,
        brd.district_name,
        brd.gp_name,
        act.activity_id AS activity_id
    FROM {{ ref ('encounters_cdc') }} AS enc
    INNER JOIN {{ ref ('subjects_cdc') }} AS sub ON enc.subject_id = sub.id
    INNER JOIN {{ ref ('bridge_dim') }} AS brd ON sub.id = brd.subjects_id
    INNER JOIN {{ ref ('activity_dim') }} AS act ON act.activity_type = enc.encounter_type
    WHERE enc.encounter_type = 'Water Quality testing'
    AND enc.observations != '{}'
), 
extract_fields AS (
    SELECT
        encounter_id,
        activity_id,
        ward_name,
        block_name,
        district_name,
        gp_name,
        username,
        meeting_date,
        CAST(CAST(observations AS JSONB) ->> 'Date of sample collection' AS DATE) AS date_sample_collection,
        CAST(CAST(observations AS JSONB) ->> 'PH' AS FLOAT) AS ph_count,
        CAST(CAST(observations AS JSONB) ->> 'Chloride' AS FLOAT) AS chloride_count,
        CAST(CAST(observations AS JSONB) ->> 'Total Hardness' AS FLOAT) AS hardness,
        CAST(CAST(observations AS JSONB) ->> 'Total Alkalnity' AS FLOAT) AS total_alkalinity,
        CAST(CAST(observations AS JSONB) ->> 'Bacteriological Contamination' AS VARCHAR) AS bacterial_contamination,
        CAST(CAST(observations AS JSONB) ->> 'Nitrate' AS FLOAT) AS nitrate_count,
        CAST(CAST(observations AS JSONB) ->> 'Iron' AS FLOAT) AS iron_count,
        CAST(CAST(observations AS JSONB) ->> 'Arsenic' AS FLOAT) AS arsenic_count,
        CAST(CAST(observations AS JSONB) ->> 'Fluoride' AS FLOAT) AS fluoride_count,
        CAST(json_extract_path_text(audit::json, 'Created at') AS TIMESTAMP) AS created_at_timestamp,
        CAST(json_extract_path_text(audit::json, 'Last modified at') AS TIMESTAMP) AS last_modified_timestamp
    FROM wq_raw_data AS raw_data
), 
parameter_values AS (
    SELECT
        encounter_id,
        date_sample_collection,
        ward_name,
        block_name,
        district_name,
        gp_name,
        activity_id,
        username,
        created_at_timestamp,
        meeting_date,
        'पीएच' AS parameter,
        ph_count AS last_test_done_value,
        '6.5-8.5' AS permissible_limits,
        '{{ invocation_id }}' AS create_audit_id
    FROM extract_fields
    UNION ALL
    SELECT
        encounter_id,
        date_sample_collection,
        ward_name,
        block_name,
        district_name,
        gp_name,
        activity_id,
        username,
        created_at_timestamp,
        meeting_date,
        'टोटल हार्डनेस ' AS parameter,
        hardness AS last_test_done_value,
        '< 600' AS permissible_limits,
        '{{ invocation_id }}' AS create_audit_id
    FROM extract_fields
    UNION ALL
    SELECT
        encounter_id,
        date_sample_collection,
        ward_name,
        block_name,
        district_name,
        gp_name,
        activity_id,
        username,
        created_at_timestamp,
        meeting_date,
        'टोटल एल्कॉनिटी' AS parameter,
        total_alkalinity AS last_test_done_value,
        '< 600' AS permissible_limits,
        '{{ invocation_id }}' AS create_audit_id
    FROM extract_fields
    UNION ALL
    SELECT
        encounter_id,
        date_sample_collection,
        ward_name,
        block_name,
        district_name,
        gp_name,
        activity_id,
        username,
        created_at_timestamp,
        meeting_date,
        'क्लोराइड' AS parameter,
        chloride_count AS last_test_done_value,
        '< 1000' AS permissible_limits,
        '{{ invocation_id }}' AS create_audit_id
    FROM extract_fields
    UNION ALL
    SELECT
        encounter_id,
        date_sample_collection,
        ward_name,
        block_name,
        district_name,
        gp_name,
        activity_id,
        username,
        created_at_timestamp,
        meeting_date,
        'Nनाइट्रेट' AS parameter,
        nitrate_count AS last_test_done_value,
        '< 45' AS permissible_limits,
        '{{ invocation_id }}' AS create_audit_id
    FROM extract_fields
    UNION ALL
    SELECT
        encounter_id,
        date_sample_collection,
        ward_name,
        block_name,
        district_name,
        gp_name,
        activity_id,
        username,
        created_at_timestamp,
        meeting_date,
        'आर्सेनिक ' AS parameter,
        arsenic_count AS last_test_done_value,
        '< 0.01' AS permissible_limits,
        '{{ invocation_id }}' AS create_audit_id
    FROM extract_fields
    UNION ALL
    SELECT
        encounter_id,
        date_sample_collection,
        ward_name,
        block_name,
        district_name,
        gp_name,
        activity_id,
        username,
        created_at_timestamp,
        meeting_date,
        'फ्लोराइड' AS parameter,
        fluoride_count AS last_test_done_value,
        '< 1.5' AS permissible_limits,
        '{{ invocation_id }}' AS create_audit_id
    FROM extract_fields
    UNION ALL
    SELECT
        encounter_id,
        date_sample_collection,
        ward_name,
        block_name,
        district_name,
        gp_name,
        activity_id,
        username,
        created_at_timestamp,
        meeting_date,
        'आयरन ' AS parameter,
        iron_count AS last_test_done_value,
        '< 1' AS permissible_limits,
        '{{ invocation_id }}' AS create_audit_id
    FROM extract_fields
    UNION ALL
    SELECT
        encounter_id,
        date_sample_collection,
        ward_name,
        block_name,
        district_name,
        gp_name,
        activity_id,
        username,
        created_at_timestamp,
        meeting_date,
        'बैक्टीरियोलॉजिकल संदूषण' AS parameter,
        CASE 
            WHEN bacterial_contamination = 'Yes' THEN 1
            ELSE 0
        END AS last_test_done_value,
        'Absent' AS permissible_limits,
        '{{ invocation_id }}' AS create_audit_id
    FROM extract_fields
)

SELECT 
    encounter_id,
    ward_name,
    block_name,
    district_name,
    gp_name,
    activity_id,
    username,
    meeting_date,
    "parameter" as "पैरामीटर",
    last_test_done_value as "अंतिम परीक्षण किया गया मान",
    permissible_limits as "अनुमेय सीमा",
    created_at_timestamp,
    create_audit_id
FROM parameter_values
