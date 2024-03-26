{{ config(materialized='incremental') }}

WITH meeting_form_responses AS (
	SELECT
        obs.id,
		obs.meeting_date,
        obs.encounter_type,
		obs.num_participants,
		obs.num_women_participants,
		obs.num_days_water_unavailable,
		obs.reasons_water_unavailable,
		obs.photos
	FROM
		{{ ref ('observations_intermediate') }} as obs 
        LEFT JOIN activity_dim as activity
            ON obs.encounter_type = activity.activity_type
)
SELECT
	*
FROM
	meeting_form_responses

	
	