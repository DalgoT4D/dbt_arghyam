{{
    config(
        materialized='incremental'
    )
}}

WITH 
    tank_cleaning_raw_data AS (
        SELECT 
            enc.subject_type
            , sub.location
            , enc.observations
            , brd.location_id
            --, act.id AS activity_id
        FROM {{ ref ('encounters_cdc') }} as enc
        LEFT JOIN {{ ref ('subjects_cdc') }} as sub ON enc.subject_id = sub.id
        INNER JOIN {{ ref ('bridge_dim') }} AS brd ON sub.id = brd.subjects_id
        --INNER JOIN {{ ref ('activity_dim') }} AS act ON act.activity_type = enc.encounter_type
        WHERE enc.encounter_type = 'Tank Cleaning'
        AND enc.observations != '{}'
)
, extract_fields AS (
    SELECT
        location_id
       -- , activity_id
        , json_extract_path_text(raw_data.observations::json, 'Date of tank cleaning') AS tank_cleaning_date
        , json_extract_path_text(raw_data.observations::json, 'Take a picture of the tank cleaning process') AS photo_tank_cleaning_process
        , json_extract_path_text(raw_data.observations::json, 'Take a picture of the proceedings of tank cleaning with WIMC members signature') AS photo_wimc_sign
        , json_extract_path_text(raw_data.observations::json, 'Take a picture of the written notification to community regarding the tank cleaning') AS photo_written_notification
        , json_extract_path_text(raw_data.observations::json, 'Remarks') AS remarks
    FROM tank_cleaning_raw_data AS raw_data
)

SELECT 
    tank_cleaning_date::timestamp::date
    , location_id
    -- , activity_id
    , ARRAY [
             photo_tank_cleaning_process
             , photo_wimc_sign
             , photo_written_notification
         ] AS photos_tank_cleaning 
    , remarks
FROM extract_fields