{{
    config(
        materialized='table',
        schema='analytics'
    )
}}

WITH 
log_book_raw_data AS (
    SELECT 
        enc.id AS encounter_id,
        enc.subject_type,
        enc.username,
        sub.location,
        enc.observations,
        enc.audit,
        brd.location_id,
        act.activity_id AS activity_id
    FROM {{ ref ('encounters_cdc') }} as enc
    INNER JOIN {{ ref ('subjects_cdc') }} as sub ON enc.subject_id = sub.id
    INNER JOIN {{ ref ('bridge_dim') }} AS brd ON sub.id = brd.subjects_id
    INNER JOIN {{ ref ('activity_dim') }} AS act ON act.activity_type = enc.encounter_type
    WHERE enc.encounter_type = 'Log book record'
    AND enc.observations != '{}'
), 
extract_fields AS (
    SELECT
        encounter_id,
        location_id,
        activity_id,
        username,
        json_extract_path_text(raw_data.observations::json, 'Reporting Year') AS reporting_year,
        json_extract_path_text(raw_data.observations::json, 'Reporting month') AS reporting_month,
        json_extract_path_text(raw_data.observations::json, 'Photo of the log-book of the entire month') AS photo_logbook,
        json_extract_path_text(raw_data.observations::json, 'What were the reasons for not supplying water') AS reasons_no_water,
        json_extract_path_text(raw_data.observations::json, 'For how many days was water not supplied in the ward') AS days_no_water,
        json_extract_path_text(raw_data.audit::json, 'Created at') AS created_at_timestamp,
        json_extract_path_text(raw_data.audit::json, 'Last modified at') AS last_modified_timestamp
    FROM log_book_raw_data AS raw_data
)

SELECT 
    -- encounter_id,
    reporting_year::int,
    reporting_month,
    location_id,
    username,
    -- activity_id,
    days_no_water::int,
    reasons_no_water,
    -- photo_logbook,
    -- created_at_timestamp,
    -- last_modified_timestamp,
    CURRENT_TIMESTAMP AS create_db_timestamp,
    '{{ invocation_id }}' AS create_audit_id
FROM extract_fields
