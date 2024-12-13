{{ config(materialized='table', schema='analytics') }}


WITH 
tank_cleaning_raw_data AS (
    SELECT 
        enc.id AS encounter_id,
        enc.subject_type,
        enc.username,
        sub.location,
        enc.observations,
        enc.audit,
        act.activity_id,
        brd.ward_name,
        brd.block_name,
        brd.district_name,
        brd.gp_name,
        enc.meeting_date
    FROM {{ ref ('dedup_enc') }} AS enc
    INNER JOIN {{ ref ('subjects_cdc') }} AS sub ON enc.subject_id = sub.id
    INNER JOIN {{ ref ('bridge_dim') }} AS brd ON sub.id = brd.subjects_id
    INNER JOIN {{ ref ('activity_dim') }} AS act ON enc.encounter_type = act.activity_type
    WHERE
        enc.encounter_type = 'Tank Cleaning'
        AND enc.observations != '{}'
),

extract_fields AS (
    SELECT
        raw_data.encounter_id,
        raw_data.meeting_date,
        raw_data.activity_id,
        raw_data.ward_name,
        raw_data.block_name,
        raw_data.district_name,
        raw_data.gp_name,
        raw_data.username,
        json_extract_path_text(raw_data.observations::json, 'Remarks') AS remarks,
        json_extract_path_text(raw_data.audit::json, 'Created at') AS created_at_timestamp,
        json_extract_path_text(raw_data.audit::json, 'Last modified at') AS last_modified_timestamp
    FROM tank_cleaning_raw_data AS raw_data
)

SELECT 
    encounter_id,
    meeting_date,
    ward_name,
    block_name,
    district_name,
    gp_name,
    username,
    remarks,
    created_at_timestamp,
    '{{ invocation_id }}' AS create_audit_id
FROM extract_fields
