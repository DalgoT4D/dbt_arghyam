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
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_meta,
        CASE 
            WHEN to_timestamp(last_modified_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') = to_timestamp(json_extract_path_text(audit::json, 'Created at'), 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') THEN 'C'
            WHEN to_timestamp(last_modified_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') >= to_timestamp(json_extract_path_text(audit::json, 'Created at'), 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') THEN 'U'
            ELSE 'NA'
        END AS op_type
    FROM {{ source('source_arghyam_surveys', 'encounters') }}
)

SELECT
    id,
    audit,
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
    op_type
FROM source
