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
    activity_type,
    -- location_id placeholder here - to be joined with another table
    '{{ invocation_id }}' AS location_id,
    CURRENT_TIMESTAMP AS create_timestamp,
    --  create_audit_id to be populated during time of load
    '{{ invocation_id }}' AS create_audit_id,
    CURRENT_TIMESTAMP AS update_timestamp,
    -- update_audit_id to be populated during time of load
    '{{ invocation_id }}' AS update_audit_id
FROM ranked_activities