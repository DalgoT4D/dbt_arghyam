{{ 
    config(
        materialized='view'
    ) 
}}
WITH cleaned_encounters AS (
	SELECT
		id,
		-- Adding meeting form responses fact fields
		CAST(CAST(observations AS JSONB) ->> 'Date of WIMC meeting' AS DATE) AS meeting_date,
		CAST(CAST(observations AS JSONB) ->> 'How many members attended the meeting' AS INT) AS num_participants,
		CAST(CAST(observations AS JSONB) ->> 'How many women participants attended the meeting' AS INT) AS num_women_participants,
		CAST(CAST(observations AS JSONB) ->> 'For how many days was water not supplied in the ward' AS INT) AS num_days_water_unavailable,
		CAST(CAST(observations AS JSONB) ->> 'What were the reasons for not supplying water_0' AS VARCHAR) AS reasons_water_unavailable,
		-- Adding water quality testing fact fields
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
		-- Dynamically aggregating all photo URLs into an array
		(
			SELECT
				ARRAY_AGG(value::text)
			FROM
				jsonb_each_text(CAST(observations AS JSONB)) -- Corrected casting here
			WHERE
				KEY LIKE 'Photo of the log-book of the entire month_%'
				OR KEY LIKE 'Take picture of the Jal Chuapal proceedings_%'
				OR KEY LIKE 'Photo of the WIMC meeting register with the minutes'
				OR KEY LIKE 'Take a picture of the meeting WIMC when there is maximum attendance') AS photos
		FROM
			{{ source('silver', 'encounters_normalized') }}
		WHERE
			observations IS NOT NULL
			AND observations::text <> '{}'
)
SELECT
	id, meeting_date, num_participants, num_women_participants, num_days_water_unavailable, reasons_water_unavailable, date_sample_collection, date_testing, ph_count, chloride_count, hardness, total_alkalinity, bacterial_contamination, nitrate_count, iron_count, arsenic_count, fluoride_count, photos
FROM
	cleaned_encounters;
