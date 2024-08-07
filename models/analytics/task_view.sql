{{ config(
    materialized='table'
) }}

WITH source AS (
    SELECT
        enc.encounter_type,
        enc.username,
        meeting_date
    FROM {{ ref('encounters_cdc') }} as enc
),

pivoted AS (
    SELECT
        meeting_date::date,
        EXTRACT(YEAR FROM meeting_date::timestamp) AS reporting_year,
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
        END as reporting_month,
        username,
        MAX(CASE WHEN encounter_type = 'WIMC meeting' THEN 'Yes' ELSE 'No' END) AS "WIMC Meeting",
        MAX(CASE WHEN encounter_type = 'Jal Chaupal' THEN 'Yes' ELSE 'No' END) AS "Jal Chaupal",
        MAX(CASE WHEN encounter_type = 'Log book record' THEN 'Yes' ELSE 'No' END) AS "Log book record"
    FROM source
    GROUP BY meeting_date, username
)

SELECT * FROM pivoted
