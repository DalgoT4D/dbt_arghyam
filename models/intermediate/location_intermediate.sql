{{ 
    config(
        materialized='view'
    ) 
}}

WITH extracted_location_data AS (
    -- Extracting from source data (JSON) and converting to structured
    SELECT
        json_extract_path_text(sub.location::json, 'Ward') AS ward_name,
        json_extract_path_text(sub.location::json, 'Block') AS block_name,
        json_extract_path_text(sub.location::json, 'District') AS district_name,
        json_extract_path_text(sub.location::json, 'Gram Panchayat') AS gp_name,
        json_extract_path_text(sub.audit::json, 'Created at') AS src_created_timestamp
    FROM
        {{ source('silver', 'subjects_normalized') }} AS sub
)

, dedup_location_data AS (
    -- De-duplicating rows
    SELECT 
        ward_name
        , block_name
        , district_name
        , gp_name
        , MIN(src_created_timestamp) AS src_created_timestamp
    FROM extracted_location_data
    GROUP BY 
        ward_name
        , block_name
        , district_name
        , gp_name
)

SELECT 
    ward_name
    , block_name
    , district_name
    , gp_name
    , src_created_timestamp
FROM dedup_location_data