{{ config(materialized='table') }}

WITH water_quality_form_responses AS (
	SELECT
        obs.id,
		obs.date_sample_collection,
        obs.date_testing,
        obs.ph_count,
        obs.chloride_count,
        obs.hardness,
        obs.total_alkalinity,
        obs.bacterial_contamination,
        obs.nitrate_count,
        obs.iron_count,
        obs.arsenic_count,
        obs.fluoride_count,
        obs.photos
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
	water_quality_form_responses

	
	