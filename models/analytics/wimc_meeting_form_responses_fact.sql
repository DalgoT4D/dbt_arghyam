{{ config(
		materialized='table',
		schema='analytics'
		) 
}}

WITH extract_data AS (
    SELECT
        enc.id, -- encounter_id
        enc.subject_id,
        enc.encounter_type,
        enc.username,
        enc.meeting_date,
        CAST(CAST(enc.observations AS JSONB) ->> 'How many members attended the meeting' AS INT) AS num_members_attended,
        CAST(CAST(enc.observations AS JSONB) ->> 'How many women participants attended the meeting' AS INT) AS num_women_participants,
        CAST(CAST(enc.observations AS JSONB) ->> 'Remarks' AS VARCHAR) AS remarks,
        JSON_EXTRACT_PATH_TEXT(CAST(enc.audit AS JSON), 'Created at') AS created_at_timestamp,
        JSON_EXTRACT_PATH_TEXT(CAST(enc.audit AS JSON), 'Last modified at') AS last_modified_timestamp,
        -- Dynamically aggregating all photo URLs into an array
        (
            SELECT ARRAY_AGG(CAST(value AS TEXT))
            FROM
                JSONB_EACH_TEXT(CAST(observations AS JSONB))
            WHERE
                key LIKE 'Take picture of the WIMC meeting register with the minutes'
                OR key LIKE 'Take a picture of the meeting WIMC when there is maximum attendance'
        ) AS photos
    FROM
        {{ ref ('dedup_enc') }} AS enc
    WHERE
        enc.observations IS NOT NULL
        AND CAST(enc.observations AS TEXT) <> '{}'
        AND enc.encounter_type = 'WIMC meeting'
    -- {% if is_incremental() %}
    -- AND TO_TIMESTAMP(json_extract_path_text(ecdc.observations::json, 'Date of WIMC meeting'), 
    -- 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') >= (SELECT MAX(meeting_date) FROM {{ this }})
    -- {% endif %}
)

SELECT
    exd.id AS encounter_id, -- id of encounters_cdc
    -- activity.activity_id, -- FK to activity_dim
    -- brd.location_id, -- same as SK of location_dim table (FK)
    exd.meeting_date,
    EXTRACT(MONTH FROM CAST(exd.meeting_date AS TIMESTAMP)) AS reporting_month,
    EXTRACT(YEAR FROM CAST(exd.meeting_date AS TIMESTAMP)) AS reporting_year,
    exd.username,
    exd.num_members_attended,
    exd.num_women_participants,
    exd.remarks,
    exd.photos,
    brd.ward_name,
    brd.block_name,
    brd.district_name,
    brd.gp_name,
    CAST(exd.created_at_timestamp AS TIMESTAMP) AS created_at_timestamp,
    -- last_modified_timestamp,
    -- CURRENT_TIMESTAMP AS create_db_timestamp,
    '{{ invocation_id }}' AS create_audit_id
FROM
    extract_data AS exd
LEFT JOIN {{ ref('activity_dim') }} AS activity ON exd.encounter_type = activity.activity_type
LEFT JOIN {{ ref ('subjects_cdc') }} AS sub ON exd.subject_id = sub.id
INNER JOIN {{ ref ('bridge_dim') }} AS brd ON sub.id = brd.subjects_id
WHERE exd.encounter_type = 'WIMC meeting'
