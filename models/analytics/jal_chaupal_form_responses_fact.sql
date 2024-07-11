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
        enc.username,
        enc.meeting_date,
        sub.location,
        enc.observations,
        enc.audit,
        brd.ward_name,
        brd.block_name,
        brd.district_name,
        brd.gp_name,
        act.activity_id AS activity_id
    FROM {{ ref ('encounters_cdc') }} as enc
    INNER JOIN {{ ref ('subjects_cdc') }} as sub ON enc.subject_id = sub.id
    INNER JOIN {{ ref ('bridge_dim') }} AS brd ON sub.id = brd.subjects_id
    INNER JOIN {{ ref ('activity_dim') }} AS act ON act.activity_type = enc.encounter_type
    WHERE enc.encounter_type = 'Jal Chaupal'
    AND enc.observations != '{}'
), 
extract_fields AS (
    SELECT
        encounter_id,
        activity_id,
        username,
        ward_name,
        block_name,
        district_name,
        gp_name,
        meeting_date,
        CASE
            WHEN json_extract_path_text(raw_data.observations::json, 'How many participants attended the meeting') = '25 - 40' THEN 40
            WHEN json_extract_path_text(raw_data.observations::json, 'How many participants attended the meeting') = 'More than 80' THEN 80
            WHEN json_extract_path_text(raw_data.observations::json, 'How many participants attended the meeting') = '60 - 80' THEN 80
            WHEN json_extract_path_text(raw_data.observations::json, 'How many participants attended the meeting') = 'Less than 25' THEN 25
            WHEN json_extract_path_text(raw_data.observations::json, 'How many participants attended the meeting') = '40 - 60' THEN 60
            ELSE NULL
        END AS num_participants,
        json_extract_path_text(raw_data.observations::json, 'How many women participants attended the meeting') AS num_women_participants,
        json_extract_path_text(raw_data.observations::json, 'Select if any of the below personal attended') AS personal_attendees,
        json_extract_path_text(raw_data.observations::json, 'Take picture of the Jal Chuapal proceedings') AS photo_proceedings,
        json_extract_path_text(raw_data.observations::json, 'Take a picture of the Jal Chaupal when there is maximum attendance') AS photo_max_attendance,
        json_extract_path_text(raw_data.observations::json, 'Remarks') AS remarks,
        CAST(json_extract_path_text(raw_data.audit::json, 'Created at') AS TIMESTAMP) AS created_at_timestamp,
        CAST(json_extract_path_text(raw_data.audit::json, 'Last modified at') AS TIMESTAMP) AS last_modified_timestamp
    FROM jal_chaupal_raw_data AS raw_data
)

SELECT 
    encounter_id,
    meeting_date,
    EXTRACT(MONTH FROM meeting_date::timestamp) AS reporting_month,
    EXTRACT(YEAR FROM meeting_date::timestamp) AS reporting_year,
    CASE 
        WHEN EXTRACT(MONTH FROM meeting_date::timestamp) = 1 THEN 'Jan'
        WHEN EXTRACT(MONTH FROM meeting_date::timestamp) = 2 THEN 'Feb'
        WHEN EXTRACT(MONTH FROM meeting_date::timestamp) = 3 THEN 'Mar'
        WHEN EXTRACT(MONTH FROM meeting_date::timestamp) = 4 THEN 'Apr'
        WHEN EXTRACT(MONTH FROM meeting_date::timestamp) = 5 THEN 'May'
        WHEN EXTRACT(MONTH FROM meeting_date::timestamp) = 6 THEN 'Jun'
        WHEN EXTRACT(MONTH FROM meeting_date::timestamp) = 7 THEN 'Jul'
        WHEN EXTRACT(MONTH FROM meeting_date::timestamp) = 8 THEN 'Aug'
        WHEN EXTRACT(MONTH FROM meeting_date::timestamp) = 9 THEN 'Sep'
        WHEN EXTRACT(MONTH FROM meeting_date::timestamp) = 10 THEN 'Oct'
        WHEN EXTRACT(MONTH FROM meeting_date::timestamp) = 11 THEN 'Nov'
        WHEN EXTRACT(MONTH FROM meeting_date::timestamp) = 12 THEN 'Dec'
    END AS meeting_month,
    username,
    ward_name,
    block_name,
    district_name,
    gp_name,
    num_participants,
    CAST(num_women_participants AS INT) AS num_women_participants,
    remarks,
    ARRAY [photo_proceedings, photo_max_attendance] AS photos_jal_chaupal,
    created_at_timestamp,
    -- last_modified_timestamp,
    -- CURRENT_TIMESTAMP AS create_db_timestamp,
    '{{ invocation_id }}' AS create_audit_id
FROM extract_fields