{{ config(
    materialized='table'
) }}

WITH source AS (
    SELECT
        to_char(date_trunc('month', to_timestamp("Encounter_date_time", 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ')), 'Mon') AS encounter_month,
        "Encounter_type" as encounter_type
    FROM {{ source('source_arghyam_surveys', 'encounters') }}
    WHERE "Encounter_type" IN ('WIMC meeting', 'Jal Chaupal', 'Log book record')
),

flagged_encounters AS (
    SELECT DISTINCT
        encounter_month,
        encounter_type,
        'Yes' AS present
    FROM source
),

pivoted AS (
    SELECT
        encounter_month,
        MAX(CASE WHEN encounter_type = 'WIMC meeting' THEN present ELSE 'No' END) AS "WIMC Meeting",
        MAX(CASE WHEN encounter_type = 'Jal Chaupal' THEN present ELSE 'No' END) AS "Jal Chaupal",
        MAX(CASE WHEN encounter_type = 'Log book record' THEN present ELSE 'No' END) AS "Log book record"
    FROM flagged_encounters
    GROUP BY encounter_month
    ORDER BY encounter_month
)

SELECT * FROM pivoted
