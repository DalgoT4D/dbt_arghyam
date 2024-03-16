{{ 
    config(
        materialized='view'
    ) 
}}

WITH extracted_location_data AS (
    SELECT 
        json_extract_path_text(sub.location::json, 'Ward') AS ward_name,
        json_extract_path_text(sub.location::json, 'Block') AS block_name,
        json_extract_path_text(sub.location::json, 'District') AS district_name,
        json_extract_path_text(sub.location::json, 'Gram Panchayat') AS gp_name
    FROM 
        {{ source('silver', 'subjects_normalized') }} AS sub
)

SELECT 
    ward_name
    , block_name
    , district_name
    , gp_name
FROM extracted_location_data
GROUP BY 
    ward_name
    , block_name
    , district_name
    , gp_name