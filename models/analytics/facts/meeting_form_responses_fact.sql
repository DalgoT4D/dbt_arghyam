-- meeting_form_responses_fact.sql
{{ config(schema='gold', materialized='view') }}
WITH cleaned_encounters AS (
SELECT
		id,
		CAST(CAST(observations AS JSONB) ->> 'Date of WIMC meeting' AS DATE) AS meeting_date,
        CAST(CAST(observations AS JSONB) ->> 'How many members attended the meeting' AS INT) AS num_participants,
        CAST(CAST(observations AS JSONB) ->> 'How many women participants attended the meeting' AS INT) AS num_women_participants, 
        CAST(CAST(observations AS JSONB) ->> 'For how many days was water not supplied in the ward' AS INT) AS num_days_water_unavailable,
        CAST(CAST(observations AS JSONB) ->> 'What were the reasons for not supplying water_0' AS VARCHAR) AS reasons_water_unavailable,
        ARRAY[
            CAST(observations as JSONB) ->> 'Take picture of the WIMC meeting register with the minutes',
            CAST(observations as JSONB) ->> 'Take a picture of the meeting WIMC when there is maximum attendance'
        ] AS photos
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
		-- Replace with actual logic to join and derive activity_id from activity dim table
--         activity_dim.id AS activity_id,
        -- Replace with actual logic to join and derive location_id from location dim table
-- 		location_dim.id AS location_id,
		num_participants,
		num_women_participants,
		num_days_water_unavailable, 
		reasons_water_unavailable,
		photos
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
-- 	and reasons_water_unavailable is not null
-- 	AND num_participants > 0
-- 	AND num_women_participants > 0 
-- 	AND num_days_water_unavailable > 0