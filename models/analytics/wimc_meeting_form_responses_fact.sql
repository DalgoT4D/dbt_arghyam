{{ config(
		materialized='table',
		schema='analytics'
		) 
}}

WITH extract_data AS (
	SELECT
		id, -- encounter_id
		subject_id,
		encounter_type,
		username,
		CAST(CAST(observations AS JSONB) ->> 'Date of WIMC meeting' AS DATE) AS meeting_date,
		CAST(CAST(observations AS JSONB) ->> 'How many members attended the meeting' AS INT) AS num_members_attended,
		CAST(CAST(observations AS JSONB) ->> 'How many women participants attended the meeting' AS INT) AS num_women_participants,
		CAST(CAST(observations AS JSONB) ->> 'Remarks' AS VARCHAR) AS remarks,
		json_extract_path_text(audit::json, 'Created at') AS created_at_timestamp,
        json_extract_path_text(audit::json, 'Last modified at') AS last_modified_timestamp,
		-- Dynamically aggregating all photo URLs into an array
		(
			SELECT
				ARRAY_AGG(value::text)
			FROM
				jsonb_each_text(CAST(observations AS JSONB))
			WHERE
				KEY LIKE 'Take picture of the WIMC meeting register with the minutes'
				OR KEY LIKE 'Take a picture of the meeting WIMC when there is maximum attendance'
				) AS photos
		FROM
			{{ ref ('encounters_cdc') }} as enc
		WHERE
			observations IS NOT NULL
			AND observations::text <> '{}'
			AND encounter_type = 'WIMC meeting'
		-- {% if is_incremental() %}
        -- AND TO_TIMESTAMP(json_extract_path_text(ecdc.observations::json, 'Date of WIMC meeting'), 
        --                 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') >= (SELECT MAX(meeting_date) FROM {{ this }})
        -- {% endif %}
)

SELECT
	-- exd.id AS encounter_id, -- id of encounters_cdc
	-- activity.activity_id, -- FK to activity_dim
	-- brd.location_id, -- same as SK of location_dim table (FK)
	exd.meeting_date,
	exd.username,
	exd.num_members_attended,
	exd.num_women_participants,
	exd.remarks,
	exd.photos,
	brd.ward_name,
	brd.block_name,
	brd.district_name,
	brd.gp_name,
	created_at_timestamp::timestamp,
    -- last_modified_timestamp,
	-- CURRENT_TIMESTAMP AS create_db_timestamp,
    '{{ invocation_id }}' AS create_audit_id
FROM
	extract_data AS exd
LEFT JOIN {{ ref('activity_dim') }} as activity ON exd.encounter_type = activity.activity_type
LEFT JOIN {{ ref ('subjects_cdc') }} as sub ON exd.subject_id = sub.id
INNER JOIN {{ ref ('bridge_dim') }} AS brd ON sub.id = brd.subjects_id
WHERE encounter_type = 'WIMC meeting'

