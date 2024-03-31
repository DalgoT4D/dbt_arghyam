{{ config(materialized='incremental',
        incremental_strategy='append'
    ) 
}}

WITH water_quality_form_responses AS (
	SELECT
        obs.id AS encounters_id,
		obs.date_sample_collection,
        obs.date_testing,
        brd.location_id, -- same as SK of location_dim table
        obs.ph_count,
        obs.chloride_count,
        obs.hardness,
        obs.total_alkalinity,
        obs.bacterial_contamination,
        obs.nitrate_count,
        obs.iron_count,
        obs.arsenic_count,
        obs.fluoride_count,
		obs.photos,
        CURRENT_TIMESTAMP AS create_db_timestamp,
        '{{ invocation_id }}' AS create_audit_id
	FROM
		{{ ref ('observations_intermediate') }} as obs 
	-- LEFT JOIN {{ ref('activity_dim') }} as activity ON obs.encounter_type = activity.activity_type
    LEFT JOIN {{ ref ('subjects_cdc') }} as sub ON obs.subject_id = sub.id
    INNER JOIN {{ ref ('bridge_dim') }} AS brd ON sub.id = brd.subjects_id
    WHERE obs.encounter_type = 'Water Quality testing'
    {% if is_incremental() %} -- to check for field that won't have a delay in reporting
        AND obs.date_sample_collection >= (SELECT CAST(MAX(create_db_timestamp) AS DATE) FROM {{ this }})
    {% endif %}
)
SELECT
	*
FROM
	water_quality_form_responses

	
	