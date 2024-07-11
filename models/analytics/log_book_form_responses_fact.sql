{{
    config(
        materialized='table',
        schema='analytics'
    )
}}

WITH 
log_book_raw_data AS (
    SELECT 
        enc.id AS encounter_id,
        enc.subject_type,
        enc.username,
        enc.meeting_date,
        sub.location,
        enc.observations,
        enc.audit,
        brd.ward_name,
        brd.block_name,
        brd.district_name,
        brd.gp_name,
        act.activity_id AS activity_id
    FROM {{ ref ('encounters_cdc') }} as enc
    INNER JOIN {{ ref ('subjects_cdc') }} as sub ON enc.subject_id = sub.id
    INNER JOIN {{ ref ('bridge_dim') }} AS brd ON sub.id = brd.subjects_id
    INNER JOIN {{ ref ('activity_dim') }} AS act ON act.activity_type = enc.encounter_type
    WHERE enc.encounter_type = 'Log book record'
    AND enc.observations != '{}'
), 
extract_fields AS (
    SELECT
        encounter_id,
        ward_name,
        block_name,
        meeting_date,
        district_name,
        gp_name,
        activity_id,
        username,
        json_extract_path_text(raw_data.observations::json, 'Reporting Year') AS reporting_year,
        json_extract_path_text(raw_data.observations::json, 'Reporting month') AS reporting_month,
        json_extract_path_text(raw_data.observations::json, 'Photo of the log-book of the entire month') AS photo_logbook,
        json_extract_path_text(raw_data.observations::json, 'What were the reasons for not supplying water') AS reasons_no_water,
        json_extract_path_text(raw_data.observations::json, 'For how many days was water not supplied in the ward') AS days_no_water,
        json_extract_path_text(raw_data.audit::json, 'Created at') AS created_at_timestamp,
        json_extract_path_text(raw_data.audit::json, 'Last modified at') AS last_modified_timestamp
    FROM log_book_raw_data AS raw_data
),
calculate_days AS (
    SELECT
        *,
        CASE 
            WHEN reporting_month IN ('Jan', 'Mar', 'May', 'Jul', 'Aug', 'Oct', 'Dec') THEN 31
            WHEN reporting_month IN ('Apr', 'Jun', 'Sep', 'Nov') THEN 30
            WHEN reporting_month = 'Feb' AND (reporting_year::int % 4 = 0 AND (reporting_year::int % 100 != 0 OR reporting_year::int % 400 = 0)) THEN 29
            WHEN reporting_month = 'Feb' THEN 28
            ELSE NULL
        END AS total_days_in_month
    FROM extract_fields
),
final_calculation AS (
    SELECT
        meeting_date,
        encounter_id,
        ward_name,
        block_name,
        district_name,
        gp_name,
        activity_id,
        username,
        reporting_year::int AS reporting_year,
        reporting_month,
        days_no_water::int AS days_no_water,
        reasons_no_water,
        created_at_timestamp,
        total_days_in_month,
        total_days_in_month - COALESCE(days_no_water::int, 0) AS days_with_water,
        CASE 
            WHEN total_days_in_month - COALESCE(days_no_water::int, 0) >= 27 THEN 'Yes'
            ELSE 'No'
        END AS met_27_days_goal,
        CURRENT_TIMESTAMP AS create_db_timestamp,
        '{{ invocation_id }}' AS create_audit_id
    FROM calculate_days
)

SELECT 
    meeting_date,
    reporting_year,
    reporting_month,
    ward_name,
    created_at_timestamp::timestamp,
    block_name,
    district_name,
    gp_name,
    username,
    days_no_water,
    reasons_no_water,
    total_days_in_month,
    days_with_water,
    met_27_days_goal,
    create_audit_id
FROM final_calculation
