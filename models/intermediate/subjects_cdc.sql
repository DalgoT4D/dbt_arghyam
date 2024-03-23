{{
    config(
        materialized='incremental'
    )
}}

SELECT
    CAST( {{ dbt_utils.generate_surrogate_key(['sub._airbyte_ab_id', 'sub.id']) }} AS VARCHAR) AS subjects_nk_id
    , sub._airbyte_ab_id AS airbyte_raw_id 
    , sub.catchments 
    , sub.id 
    , sub.encounters 
    , sub.voided 
    , sub.registration_location 
    , TO_TIMESTAMP(sub.last_modified_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') AS last_modified_at
    , sub.location 
    , sub.audit 
    , sub.observations 
    , CAST(sub.registration_date AS DATE)
    , CASE 
        WHEN TO_TIMESTAMP(sub.last_modified_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') = TO_TIMESTAMP(json_extract_path_text(sub.audit::json, 'Created at'), 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') THEN 'C'
        WHEN TO_TIMESTAMP(sub.last_modified_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') >= TO_TIMESTAMP(json_extract_path_text(sub.audit::json, 'Created at'), 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') THEN 'U'
        -- How are deletes handled?
        ELSE 'NA'
    END AS op_type
FROM {{ source('silver', 'subjects_normalized') }} AS sub
-- LEFT JOIN {{ source('silver', 'subjects_cdc') }} AS tgt ON tgt.airbyte_raw_id = sub._airbyte_ab_id

{% if is_incremental() %}
  WHERE TO_TIMESTAMP(sub.last_modified_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') >= (SELECT MAX(last_modified_at) FROM {{ this }})
{% endif %}