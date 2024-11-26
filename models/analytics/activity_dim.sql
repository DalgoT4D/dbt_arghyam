{{ config(materialized='table') }} -- Follows SCD 0
{{ config(materialized='table') }}
-- Follows SCD 0

WITH unique_activities AS (
    SELECT DISTINCT encounter_type AS activity_type
    FROM {{ ref('encounters_cdc') }}
),

ranked_activities AS (
    SELECT 
        CAST({{ dbt_utils.generate_surrogate_key(['activity_type']) }} AS VARCHAR) AS activity_id,
        activity_type
    FROM unique_activities
)

SELECT 
    activity_id,
    activity_type,
    CURRENT_TIMESTAMP AS create_db_timestamp,
    '{{ invocation_id if invocation_id is defined else "No ID Provided" }}' AS create_audit_id,  -- Handle undefined invocation_id
    1 AS is_active  -- Set to 1 (True) for active activity types
    /* Additional logic for manually updating inactive activity types can go here */
FROM ranked_activities
