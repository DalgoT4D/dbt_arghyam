{{ config(
    materialized='table',
) }}

WITH source AS (
    SELECT
        id,
        audit,
        "Groups" AS groups,
        "Voided" AS voided,
        location,
        relatives,
        catchments,
        encounters,
        enrolments,
        "External_ID" AS external_id,
        "Subject_type" AS subject_type,
        observations,
        "Registration_location" AS registration_location,
        to_timestamp(last_modified_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') AS last_modified_at,
        to_timestamp("Registration_date", 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') AS registration_date,
        json_extract_path_text(audit::json, 'Created by') AS username,
        CASE 
            WHEN
                to_timestamp(last_modified_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ')
                = to_timestamp(json_extract_path_text(audit::json, 'Created at'), 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ')
                THEN 'C'
            WHEN
                to_timestamp(last_modified_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ')
                >= to_timestamp(json_extract_path_text(audit::json, 'Created at'), 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ')
                THEN 'U'
            ELSE 'NA'
        END AS op_type
    FROM {{ source('source_arghyam_surveys', 'subjects') }}
)

SELECT
    id,
    audit,
    username,
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
    op_type
FROM source
