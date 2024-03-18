-- meeting_form_responses_fact.sql
{{ config(schema='gold', materialized='view') }}
WITH cleaned_encounters AS (
SELECT
		id,
		CAST(CAST(observations AS JSONB) ->> 'Date of WIMC meeting' AS DATE) AS meeting_date,
        CAST(CAST(observations AS JSONB) ->> 'How many members attended the meeting' AS INT) AS num_participants,
        CAST(CAST(observations AS JSONB) ->> 'How many women participants attended the meeting' AS INT) AS num_women_participants
        -- Additional fields to be included 
	FROM
		intermediate.encounters_normalized
		-- Ensure observations is not null or empty '{}'
	WHERE
		observations IS NOT NULL
		AND observations::text <> '{}'
),
transformed_encounters AS (
	SELECT
		meeting_date,
		num_participants,
		num_women_participants
	FROM
		cleaned_encounters
		--     LEFT JOIN activity_dim ON ...
		--     LEFT JOIN location_dim ON ...
		--     -- Include additional JOINs as necessary
)
SELECT
	*
FROM
	transformed_encounters
WHERE 
	meeting_date is not null 
	AND num_participants > 0
	AND num_women_participants > 0 