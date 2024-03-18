-- meeting_form_responses_fact.sql
{{ config(schema='gold', materialized='view') }}

WITH cleaned_encounters AS (
    SELECT
        id,
        CAST(observations ->> 'Date of WIMC meeting' AS DATE) AS meeting_date,
        CAST(observations ->> 'How many members attended the meeting' AS INT) AS num_participants,
        CAST(observations ->> 'How many women participants attended the meeting' AS INT) AS num_women_participants,
        -- Assuming photos data structure from avni are stored in an array format; adapt based on actual structure
        ARRAY[
            observations ->> 'Take picture of the WIMC meeting register with the minutes',
            observations ->> 'Take a picture of the meeting WIMC when there is maximum attendance'
        ] AS photos,
        -- To add additional fields
        ...
    FROM intermediate.encounters
    -- Ensure observations is not null or empty '{}'
    WHERE observations IS NOT NULL AND observations::text <> '{}'
),
transformed_encounters AS (
    SELECT
        meeting_date,
        -- Replace with actual logic to join and derive activity_id from another table
        activity_dim.id AS activity_id,
        -- Replace with actual logic to join and derive location_id from another table
        location_dim.id AS location_id,
        num_participants,
        num_women_participants,
        -- Handle additional logic for other fields as necessary
        photos,
        ...
    FROM cleaned_encounters
    LEFT JOIN activity_dim ON ...
    LEFT JOIN location_dim ON ...
    -- Include additional JOINs as necessary
)
SELECT * FROM transformed_encounters
