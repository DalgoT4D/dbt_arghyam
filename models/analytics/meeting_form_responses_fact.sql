{{ config(materialized='table') }}

WITH transformed_encounters AS (
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
	{{ ref ('meeting_form_responses_intermediate') }} AS sub

	
	