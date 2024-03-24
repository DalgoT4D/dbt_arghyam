{{
    config(
        materialized='table'
    )
}}

WITH extract_raw_data AS (
    select 
        sub.id AS subjects_id, 
        sub.location AS location_json,
        json_extract_path_text(sub.location::json, 'Ward') AS ward_name,
        json_extract_path_text(sub.location::json, 'Block') AS block_name,
        json_extract_path_text(sub.location::json, 'District') AS district_name,
        json_extract_path_text(sub.location::json, 'Gram Panchayat') AS gp_name
    from {{ ref ('subjects_cdc') }} AS sub
)

SELECT 
    subjects_id,
    CAST( {{ dbt_utils.generate_surrogate_key(['ward_name', 'block_name', 'district_name', 'gp_name']) }} AS VARCHAR) AS location_id
FROM extract_raw_data
