{{ config(
    materialized='table'
) }}

WITH source AS (
    SELECT
        "ID" AS id,
        audit,
        "Voided" AS voided,
        "Subject_ID" AS subject_id,
        "External_ID" AS external_id,
        "Subject_type" AS subject_type,
        observations,
        "Encounter_type" AS encounter_type,
        "Cancel_location" AS cancel_location,
        "Encounter_location" AS encounter_location,
        "cancelObservations" AS cancel_observations,
        "Subject_external_ID" AS subject_external_id,
        to_timestamp(
            "Cancel_date_time", 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ'
        ) AS cancel_date_time,
        to_timestamp(
            last_modified_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ'
        ) AS last_modified_at,
        to_timestamp(
            "Max_scheduled_date", 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ'
        ) AS max_scheduled_date,
        to_timestamp(
            "Encounter_date_time", 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ'
        ) AS encounter_date_time,
        to_timestamp(
            "Earliest_scheduled_date", 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ'
        ) AS earliest_scheduled_date,
        json_extract_path_text(audit::json, 'Created by') AS username,
        CASE
            WHEN
                to_timestamp(
                    last_modified_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ'
                )
                = to_timestamp(
                    json_extract_path_text(audit::json, 'Created at'),
                    'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ'
                )
                THEN 'C'
            WHEN
                to_timestamp(
                    last_modified_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ'
                )
                >= to_timestamp(
                    json_extract_path_text(audit::json, 'Created at'),
                    'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ'
                )
                THEN 'U'
            ELSE 'NA'
        END AS op_type,
        CASE
            WHEN observations = '{}' THEN NULL
            WHEN
                "Encounter_type" = 'WIMC meeting'
                THEN (observations::jsonb ->> 'Date of WIMC meeting')::date
            WHEN
                "Encounter_type" = 'Jal Chaupal'
                THEN (observations::jsonb ->> 'Date of jal chaupal')::date
            WHEN
                "Encounter_type" = 'Water Quality testing'
                THEN (observations::jsonb ->> 'Date of testing')::date
            WHEN
                "Encounter_type" = 'Tank Cleaning'
                THEN (observations::jsonb ->> 'Date of tank cleaning')::date
            WHEN "Encounter_type" = 'Log book record'
                THEN
                    to_date(
                        concat(
                            json_extract_path_text(
                                observations::json, 'Reporting Year'
                            ),
                            '-',
                            CASE json_extract_path_text(
                                observations::json, 'Reporting month'
                            )
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
