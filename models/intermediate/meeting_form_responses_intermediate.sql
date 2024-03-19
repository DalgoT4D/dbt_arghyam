{{ 
    config(
        materialized='view'
    ) 
}}
WITH cleaned_encounters AS (
	SELECT
		id,
		CAST(CAST(observations AS JSONB) ->> 'Date of WIMC meeting' AS DATE) AS meeting_date,
		CAST(CAST(observations AS JSONB) ->> 'How many members attended the meeting' AS INT) AS num_participants,
		CAST(CAST(observations AS JSONB) ->> 'How many women participants attended the meeting' AS INT) AS num_women_participants,
		CAST(CAST(observations AS JSONB) ->> 'For how many days was water not supplied in the ward' AS INT) AS num_days_water_unavailable,
		CAST(CAST(observations AS JSONB) ->> 'What were the reasons for not supplying water_0' AS VARCHAR) AS reasons_water_unavailable,
		ARRAY [
             CAST(observations as JSONB) ->> 'Take picture of the WIMC meeting register with the minutes',
             CAST(observations as JSONB) ->> 'Take a picture of the meeting WIMC when there is maximum attendance'
         ] AS photos
	FROM
		intermediate.encounters_normalized
		-- Ensure observations is not null or empty '{}'
	WHERE
		observations IS NOT NULL
		AND observations::text <> '{}'
),
transformed_encounters AS (
	SELECT
		meeting_date,
		num_participants,
		num_women_participants,
		num_days_water_unavailable,
		reasons_water_unavailable,
		photos
	FROM
		cleaned_encounters
    GROUP BY 1,2,3,4,5,6
)
SELECT
	meeting_date,
    num_participants,
    num_women_participants,
    num_days_water_unavailable,
    reasons_water_unavailable,
    photos
FROM
	transformed_encounters