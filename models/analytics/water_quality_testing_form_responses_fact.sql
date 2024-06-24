{{
    config(
        materialized='table',
        schema='analytics'
    )
}}

WITH 
    wq_raw_data AS (
        SELECT 
            enc.id AS encounter_id
            , enc.subject_type
            , enc.username
            , sub.location
            , enc.observations
            , enc.audit
            , brd.location_id -- same as SK of location_dim table
            , act.activity_id AS activity_id
        FROM {{ ref ('encounters_cdc') }} as enc
        INNER JOIN {{ ref ('subjects_cdc') }} as sub ON enc.subject_id = sub.id
        INNER JOIN {{ ref ('bridge_dim') }} AS brd ON sub.id = brd.subjects_id
        INNER JOIN {{ ref ('activity_dim') }} AS act ON act.activity_type = enc.encounter_type
        WHERE enc.encounter_type = 'Water Quality testing'
        AND enc.observations != '{}'
        -- {% if is_incremental() %}
        -- AND TO_TIMESTAMP(json_extract_path_text(raw_data.observations::json, 'Date of tank cleaning'), 
        --                 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') >= (SELECT MAX(tank_cleaning_date) FROM {{ this }})
        -- {% endif %}
)
, extract_fields AS (
    SELECT
        encounter_id,
        location_id,
        activity_id,
        username,
        CAST(CAST(observations AS JSONB) ->> 'Date of sample collection' AS DATE) AS date_sample_collection,
		CAST(CAST(observations AS JSONB) ->> 'Date of testing' AS DATE) AS date_testing,
		CAST(CAST(observations AS JSONB) ->> 'PH' AS FLOAT) AS ph_count,
		CAST(CAST(observations AS JSONB) ->> 'Chloride' AS FLOAT) AS chloride_count,
		CAST(CAST(observations AS JSONB) ->> 'Total Hardness' AS FLOAT) AS hardness,
		CAST(CAST(observations AS JSONB) ->> 'Total Alkalnity' AS FLOAT) AS total_alkalinity,
		CAST(CAST(observations AS JSONB) ->> 'Bacteriological Contamination' AS VARCHAR) AS bacterial_contamination,
		CAST(CAST(observations AS JSONB) ->> 'Nitrate' AS FLOAT) AS nitrate_count,
		CAST(CAST(observations AS JSONB) ->> 'Iron' AS FLOAT) AS iron_count,
		CAST(CAST(observations AS JSONB) ->> 'Arsenic' AS FLOAT) AS arsenic_count,
		CAST(CAST(observations AS JSONB) ->> 'Fluoride' AS FLOAT) AS fluoride_count,
        json_extract_path_text(raw_data.audit::json, 'Created at') AS created_at_timestamp,
        json_extract_path_text(raw_data.audit::json, 'Last modified at') AS last_modified_timestamp
    FROM wq_raw_data AS raw_data
)

SELECT 
    -- encounter_id
    -- , location_id
    -- , activity_id
    date_sample_collection
    , username
    , date_testing
    , ph_count
    , chloride_count
    , hardness
    , total_alkalinity
    , bacterial_contamination
    , nitrate_count
    , iron_count
    , arsenic_count
    , fluoride_count
    -- , created_at_timestamp
    -- , last_modified_timestamp
    -- , CURRENT_TIMESTAMP AS create_db_timestamp
    , '{{ invocation_id }}' AS create_audit_id
FROM extract_fields