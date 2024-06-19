{{ config(materialized='table', schema='analytics') }}
 -- Follows SCD 0

WITH unique_activities AS (
    SELECT
        DISTINCT encounter_type AS activity_type
    FROM {{ ref ('encounters_cdc') }} enc

),
ranked_activities AS (
    SELECT
        CAST( {{ dbt_utils.generate_surrogate_key(['activity_type']) }} AS VARCHAR) AS activity_id,
        activity_type
    FROM unique_activities
)
SELECT
    activity_id
    , activity_type
    , CURRENT_TIMESTAMP AS create_db_timestamp
    , '{{ invocation_id }}' AS create_audit_id
    , 1 AS is_active
    /*CASE -- manually updated with 1 (True) or 0 (False) when an activity type becomes inactive*/
FROM ranked_activities