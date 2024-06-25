{{ config(materialized='table', schema='analytics') }}


WITH 
    tank_cleaning_raw_data AS (
        SELECT 
            enc.id as encounter_id,
            enc.subject_type
            , enc.username
            , sub.location
            , enc.observations
            , enc.audit
            , act.activity_id AS activity_id
            , brd.ward_name,
            brd.block_name,
            brd.district_name,
            brd.gp_name
        FROM {{ ref ('encounters_cdc') }} as enc
        INNER JOIN {{ ref ('subjects_cdc') }} as sub ON enc.subject_id = sub.id
        INNER JOIN {{ ref ('bridge_dim') }} AS brd ON sub.id = brd.subjects_id
        INNER JOIN {{ ref ('activity_dim') }} AS act ON act.activity_type = enc.encounter_type
        WHERE enc.encounter_type = 'Tank Cleaning'
        AND enc.observations != '{}'
)
, extract_fields AS (
    SELECT
        encounter_id,
        activity_id,
        ward_name,
        block_name,
        district_name,
        gp_name
        , username
        , json_extract_path_text(raw_data.observations::json, 'Date of tank cleaning') AS tank_cleaning_date
        , json_extract_path_text(raw_data.observations::json, 'Remarks') AS remarks
        , json_extract_path_text(raw_data.audit::json, 'Created at') AS created_at_timestamp
        , json_extract_path_text(raw_data.audit::json, 'Last modified at') AS last_modified_timestamp
    FROM tank_cleaning_raw_data AS raw_data
)

SELECT 
    encounter_id,
    tank_cleaning_date::timestamp::date,
    ward_name,
    block_name,
    district_name,
    gp_name
    , username
    , remarks
    , created_at_timestamp
    , '{{ invocation_id }}' AS create_audit_id
FROM extract_fields