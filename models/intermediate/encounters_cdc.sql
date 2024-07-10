{{ config(
    materialized='table'
) }}

WITH source AS (
    SELECT
        "ID" as id,
        audit,
        "Voided" as voided,
        "Subject_ID" as subject_id,
        "External_ID" as external_id,
        "Subject_type" as subject_type,
        observations,
        "Encounter_type" as encounter_type,
        "Cancel_location" as cancel_location,
        to_timestamp("Cancel_date_time", 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') as cancel_date_time,
        to_timestamp(last_modified_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') as last_modified_at,
        "Encounter_location" as encounter_location,
        to_timestamp("Max_scheduled_date", 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') as max_scheduled_date,
        "cancelObservations" as cancel_observations,
        to_timestamp("Encounter_date_time", 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') as encounter_date_time,
        "Subject_external_ID" as subject_external_id,
        to_timestamp("Earliest_scheduled_date", 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') as earliest_scheduled_date,
        json_extract_path_text(audit::json, 'Created by') as username,
        CASE 
            WHEN to_timestamp(last_modified_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') = to_timestamp(json_extract_path_text(audit::json, 'Created at'), 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') THEN 'C'
            WHEN to_timestamp(last_modified_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') >= to_timestamp(json_extract_path_text(audit::json, 'Created at'), 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') THEN 'U'
            ELSE 'NA'
        END AS op_type,
        CASE 
            WHEN observations = '{}' THEN NULL
            WHEN "Encounter_type" = 'WIMC meeting' THEN CAST(CAST(observations AS JSONB) ->> 'Date of WIMC meeting' AS DATE)
            WHEN "Encounter_type" = 'Jal Chaupal' THEN CAST(CAST(observations AS JSONB) ->> 'Date of jal chaupal' AS DATE)
            WHEN "Encounter_type" = 'Water Quality testing' THEN CAST(CAST(observations AS JSONB) ->> 'Date of testing' AS DATE)
            WHEN "Encounter_type" = 'Tank Cleaning' THEN CAST(CAST(observations AS JSONB) ->> 'Date of tank cleaning' AS DATE)
            WHEN "Encounter_type" = 'Log book record' THEN 
                TO_DATE(
                    CONCAT(
                        json_extract_path_text(observations::json, 'Reporting Year'), '-',
                        CASE json_extract_path_text(observations::json, 'Reporting month')
                            WHEN 'Jan' THEN '01'
                            WHEN 'Feb' THEN '02'
                            WHEN 'Mar' THEN '03'
                            WHEN 'Apr' THEN '04'
                            WHEN 'May' THEN '05'
                            WHEN 'Jun' THEN '06'
                            WHEN 'Jul' THEN '07'
                            WHEN 'Aug' THEN '08'
                            WHEN 'Sep' THEN '09'
                            WHEN 'Oct' THEN '10'
                            WHEN 'Nov' THEN '11'
                            WHEN 'Dec' THEN '12'
                            ELSE '00'
                        END,
                        '-01'
                    ),
                    'YYYY-MM-DD'
                )
            ELSE NULL
        END AS meeting_date
    FROM {{ source('source_arghyam_surveys', 'encounters') }}
)

SELECT
    id,
    audit,
    username,
    voided,
    subject_id,
    external_id,
    subject_type,
    observations,
    encounter_type,
    cancel_location,
    cancel_date_time,
    last_modified_at,
    encounter_location,
    max_scheduled_date,
    cancel_observations,
    encounter_date_time,
    subject_external_id,
    earliest_scheduled_date,
    op_type,
    meeting_date
FROM source
