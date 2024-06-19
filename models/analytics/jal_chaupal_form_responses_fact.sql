{{
    config(
        materialized='table',
        schema='analytics'
    )
}}

WITH 
    jal_chaupal_raw_data AS (
        SELECT 
            enc.id AS encounter_id,
            enc.subject_type,
            sub.location,
            enc.observations,
            enc.audit,
            brd.location_id, -- same as SK of location_dim table,
            act.activity_id AS activity_id
        FROM {{ ref ('encounters_cdc') }} as enc
        INNER JOIN {{ ref ('subjects_cdc') }} as sub ON enc.subject_id = sub.id
        INNER JOIN {{ ref ('bridge_dim') }} AS brd ON sub.id = brd.subjects_id
        INNER JOIN {{ ref ('activity_dim') }} AS act ON act.activity_type = enc.encounter_type
        WHERE enc.encounter_type = 'Jal Chaupal'
        AND enc.observations != '{}'
        -- {% if is_incremental() %}
        -- AND TO_TIMESTAMP(json_extract_path_text(raw_data.observations::json, 'Date of tank cleaning'), 
        --                 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') >= (SELECT MAX(meeting_date) FROM {{ this }})
        -- {% endif %}
), 
extract_fields AS (
    SELECT
	    encounter_id,
        location_id,
        activity_id,
        json_extract_path_text(raw_data.observations::json, 'Date of jal chaupal') AS meeting_date,
        json_extract_path_text(raw_data.observations::json, 'How many participants attended the meeting') AS num_participants,
        json_extract_path_text(raw_data.observations::json, 'How many women participants attended the meeting') AS num_women_participants,
        json_extract_path_text(raw_data.observations::json, 'Select if any of the below personal attended') AS personal_attendees,
        json_extract_path_text(raw_data.observations::json, 'Take picture of the Jal Chuapal proceedings') AS photo_proceedings,
        json_extract_path_text(raw_data.observations::json, 'Take a picture of the Jal Chaupal when there is maximum attendance') AS photo_max_attendance,
        json_extract_path_text(raw_data.observations::json, 'Remarks') AS remarks,
        json_extract_path_text(raw_data.audit::json, 'Created at') AS created_at_timestamp,
        json_extract_path_text(raw_data.audit::json, 'Last modified at') AS last_modified_timestamp
    FROM jal_chaupal_raw_data AS raw_data
)

SELECT 
	encounter_id,
    meeting_date::timestamp::date,
    location_id,
    activity_id,
    num_participants,
    CAST(num_women_participants AS INT) AS num_women_participants,
    remarks,
    ARRAY [photo_proceedings, photo_max_attendance] AS photos_jal_chaupal,
    created_at_timestamp,
    last_modified_timestamp,
    CURRENT_TIMESTAMP AS create_db_timestamp,
    '{{ invocation_id }}' AS create_audit_id
FROM extract_fields