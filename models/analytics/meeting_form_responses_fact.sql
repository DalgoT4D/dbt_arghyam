{{ config(
		materialized='incremental',
		incremental_strategy='append'
		) 
}}

WITH meeting_form_responses AS (
	SELECT
        obs.id, -- id of encounters_cdc
		activity.activity_id, -- FK to activity_dim
		brd.location_id, -- same as SK of location_dim table (FK)
		obs.meeting_date,
        obs.encounter_type,
		obs.num_participants,
		obs.num_women_participants,
		obs.num_days_water_unavailable,
		obs.reasons_water_unavailable,
		obs.photos
	FROM
		{{ ref ('observations_intermediate') }} as obs 
	LEFT JOIN {{ ref('activity_dim') }} as activity ON obs.encounter_type = activity.activity_type
	LEFT JOIN {{ ref ('subjects_cdc') }} as sub ON obs.subject_id = sub.id
    INNER JOIN {{ ref ('bridge_dim') }} AS brd ON sub.id = brd.subjects_id
	WHERE encounter_type IN ('WIMC meeting','Jal Chaupal')
)
SELECT
	*
FROM
	meeting_form_responses

	
	