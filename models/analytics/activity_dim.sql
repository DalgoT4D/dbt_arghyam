{{ config(materialized='table') }}

WITH unique_activities AS (
    SELECT
        DISTINCT encounter_type AS activity_type
    FROM intermediate.encounters_normalized
),
ranked_activities AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY activity_type) AS id,
        activity_type
    FROM unique_activities
)
SELECT
    id,
    activity_type
    , CURRENT_TIMESTAMP AS create_db_timestamp
    , '{{ invocation_id }}' AS create_audit_id
    , CURRENT_TIMESTAMP AS last_updated_timestamp
    , '{{ invocation_id }}' AS update_audit_id 
    , 1 AS is_active
    /*CASE -- manually updated with 1 (True) or 0 (False) when an activity type becomes inactive*/
FROM ranked_activities