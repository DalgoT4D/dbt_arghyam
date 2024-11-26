{{ config(
    materialized='table'
) }}

WITH source AS (
    SELECT
        enc.encounter_type,
        enc.username,
        enc.meeting_date
    FROM {{ ref('encounters_cdc') }} AS enc
    WHERE enc.meeting_date IS NOT NULL
),

pivoted AS (
    SELECT
        meeting_date::date,
        username,
        EXTRACT(YEAR FROM meeting_date::timestamp) AS "वर्ष",
        CASE
            WHEN EXTRACT(MONTH FROM meeting_date::timestamp) = 1 THEN 'Jan'
            WHEN EXTRACT(MONTH FROM meeting_date::timestamp) = 2 THEN 'Feb'
            WHEN EXTRACT(MONTH FROM meeting_date::timestamp) = 3 THEN 'Mar'
            WHEN EXTRACT(MONTH FROM meeting_date::timestamp) = 4 THEN 'Apr'
            WHEN EXTRACT(MONTH FROM meeting_date::timestamp) = 5 THEN 'May'
            WHEN EXTRACT(MONTH FROM meeting_date::timestamp) = 6 THEN 'Jun'
            WHEN EXTRACT(MONTH FROM meeting_date::timestamp) = 7 THEN 'Jul'
            WHEN EXTRACT(MONTH FROM meeting_date::timestamp) = 8 THEN 'Aug'
            WHEN EXTRACT(MONTH FROM meeting_date::timestamp) = 9 THEN 'Sep'
            WHEN EXTRACT(MONTH FROM meeting_date::timestamp) = 10 THEN 'Oct'
            WHEN EXTRACT(MONTH FROM meeting_date::timestamp) = 11 THEN 'Nov'
            WHEN EXTRACT(MONTH FROM meeting_date::timestamp) = 12 THEN 'Dec'
        END AS reporting_month,
        MAX(
            CASE WHEN encounter_type = 'WIMC meeting' THEN 'Yes' ELSE 'No' END
        ) AS "WIMC Meeting",
        MAX(
            CASE WHEN encounter_type = 'Jal Chaupal' THEN 'Yes' ELSE 'No' END
        ) AS "Jal Chaupal",
        MAX(
            CASE
                WHEN encounter_type = 'Log book record' THEN 'Yes' ELSE 'No'
            END
        ) AS "Log book record",
        MAX(
            CASE
                WHEN encounter_type = 'Water Quality testing' THEN 'Yes' ELSE
                    'No'
            END
        ) AS "Water Quality Testing",
        MAX(
            CASE WHEN encounter_type = 'Tank Cleaning' THEN 'Yes' ELSE 'No' END
        ) AS "Tank Cleaning"
    FROM source
    GROUP BY meeting_date, username
)

SELECT * FROM pivoted
