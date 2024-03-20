{{ config(materialized='table') }}

WITH meeting_form_responses AS (
	SELECT
        obs.id,
		obs.meeting_date,
		-- Replace with actual logic to join and derive activity_id from activity dim table
		--         activity_dim.id AS activity_id,
		-- Replace with actual logic to join and derive location_id from location dim table
		-- 		location_dim.id AS location_id,
		obs.num_participants,
		obs.num_women_participants,
		obs.num_days_water_unavailable,
		obs.reasons_water_unavailable,
		obs.photos
	FROM
		{{ ref ('observations_intermediate') }} as obs 
		--     LEFT JOIN activity_dim ON ...
		--     LEFT JOIN location_dim ON ...
		--     -- Include additional JOINs as necessary
)
SELECT
	*
FROM
	meeting_form_responses

	
	