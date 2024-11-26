{{
    config(
        materialized='table'
    )
}}

WITH extract_raw_data AS (
    SELECT 
        sub.id AS subjects_id, 
        sub.location AS location_json,
        json_extract_path_text(sub.location::json, 'Ward') AS ward_name,
        json_extract_path_text(sub.location::json, 'Block') AS block_name,
        json_extract_path_text(sub.location::json, 'District') AS district_name,
        json_extract_path_text(sub.location::json, 'Gram Panchayat') AS gp_name
    FROM {{ ref ('subjects_cdc') }} AS sub
),

transform_keys AS (
    SELECT 
        subjects_id,
        ward_name,
        block_name,
        district_name,
        gp_name
    FROM extract_raw_data
)

SELECT
    cast( {{ dbt_utils.generate_surrogate_key(['subjects_id']) }} AS varchar) AS id,
    subjects_id,
    ward_name,
    block_name,
    district_name,
    gp_name
FROM transform_keys
