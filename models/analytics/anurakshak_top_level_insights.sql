{{ config(materialized='view') }}

WITH tank_cleaning AS (
    SELECT
        CASE
            WHEN MAX(meeting_date) >= CURRENT_DATE - INTERVAL '6 months' THEN 'Yes'
            ELSE 'No'
        END AS tank_cleaning_done_last_6_months
    FROM
        {{ ref('meeting_form_responses_fact') }}
    WHERE
        encounter_type = 'Tank Cleaning'
),

water_quality_testing AS (
    SELECT
        CASE
            WHEN MAX(date_sample_collection) >= CURRENT_DATE - INTERVAL '6 months' THEN 'Yes'
            ELSE 'No'
        END AS water_quality_testing_done_last_6_months
    FROM
        {{ ref('water_quality_testing_form_responses_fact') }}
),

jal_chaupals_count AS (
    SELECT
        COUNT(*) AS num_jal_chaupals_last_3_months
    FROM
        {{ ref('meeting_form_responses_fact') }}
    WHERE
        encounter_type = 'Jal Chaupal'
        AND meeting_date >= CURRENT_DATE - INTERVAL '3 months'
),

wimcs_count AS (
    SELECT
        COUNT(*) AS num_wimcs_last_3_months
    FROM
        {{ ref('meeting_form_responses_fact') }}
    WHERE
        encounter_type = 'WIMC'
        AND meeting_date >= CURRENT_DATE - INTERVAL '3 months'
)

SELECT
    tc.tank_cleaning_done_last_6_months,
    wqt.water_quality_testing_done_last_6_months,
    jc.num_jal_chaupals_last_3_months,
    wimc.num_wimcs_last_3_months
FROM
    tank_cleaning tc,
    water_quality_testing wqt,
    jal_chaupals_count jc,
    wimcs_count wimc
