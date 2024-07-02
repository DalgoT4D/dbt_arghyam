{{
    config(
        materialized='table'
    )
}}

WITH extracted_location_data AS (
    SELECT 
        json_extract_path_text(sub.location::json, 'Ward') AS ward_name,
        json_extract_path_text(sub.location::json, 'Block') AS block_name,
        json_extract_path_text(sub.location::json, 'District') AS district_name,
        json_extract_path_text(sub.location::json, 'Gram Panchayat') AS gp_name,
        TO_TIMESTAMP(json_extract_path_text(sub.audit::json, 'Created at'), 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') AS src_created_timestamp,
        TO_TIMESTAMP(sub.last_modified_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') AS last_modified_at
    FROM {{ source('source_arghyam_surveys', 'subjects') }} AS sub

    {% if is_incremental() %}
    WHERE TO_TIMESTAMP(sub.last_modified_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') >= (SELECT MAX(src_last_modified_at) FROM {{ this }})
    {% endif %}

)

, dedup_location_data AS (
    -- De-duplicating rows
    SELECT 
        ward_name
        , block_name
        , district_name
        , gp_name
        , MIN(src_created_timestamp) AS src_created_timestamp
        , MAX(last_modified_at) AS src_last_modified_at
    FROM extracted_location_data
    GROUP BY 
        ward_name
        , block_name
        , district_name
        , gp_name
)

SELECT 
    CAST( {{ dbt_utils.generate_surrogate_key(['ward_name', 'block_name', 'district_name', 'gp_name']) }} AS VARCHAR) AS location_id
    , ward_name
    , block_name
    , district_name
    , gp_name
    , src_created_timestamp
    , src_last_modified_at
    , CASE 
        WHEN src_last_modified_at = src_created_timestamp THEN 'C'
        WHEN src_last_modified_at >= src_created_timestamp THEN 'U'
        -- How are deletes handled?
        ELSE 'NA'
    END AS op_type
FROM dedup_location_data
  