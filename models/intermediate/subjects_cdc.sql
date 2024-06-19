{{ config(
    materialized='table',
) }}

WITH source AS (
    SELECT
        "ID" as id,
        audit,
        "Groups" as groups,
        "Voided" as voided,
        location,
        relatives,
        catchments,
        encounters,
        enrolments,
        "External_ID" as external_id,
        "Subject_type" as subject_type,
        observations,
        to_timestamp(last_modified_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') as last_modified_at,
        to_timestamp("Registration_date", 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') as registration_date,
        "Registration_location" as registration_location,
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_meta,
        CASE 
            WHEN to_timestamp(last_modified_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') = to_timestamp(json_extract_path_text(audit::json, 'Created at'), 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') THEN 'C'
            WHEN to_timestamp(last_modified_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') >= to_timestamp(json_extract_path_text(audit::json, 'Created at'), 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') THEN 'U'
            ELSE 'NA'
        END AS op_type
    FROM {{ source('source_arghyam_surveys', 'subjects') }}
)

SELECT
    id,
    audit,
    groups,
    voided,
    location,
    relatives,
    catchments,
    encounters,
    enrolments,
    external_id,
    subject_type,
    observations,
    last_modified_at,
    registration_date,
    registration_location,
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_meta,
    op_type
FROM source
