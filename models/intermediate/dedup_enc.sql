{{ 
    config(
        materialized='table'
    )
}}

WITH deduplicated_data AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY  
                username, 
                voided,  
                external_id, 
                subject_type, 
                observations, 
                encounter_type, 
                cancel_location, 
                cancel_date_time, 
                encounter_location, 
                max_scheduled_date, 
                cancel_observations, 
                encounter_date_time, 
                subject_external_id, 
                earliest_scheduled_date, 
                op_type, 
                meeting_date
            ORDER BY last_modified_at DESC
        ) AS row_number
    FROM {{ ref('encounters_cdc') }}  -- Replace with your actual table name
)

SELECT *
FROM deduplicated_data
WHERE row_number = 1
