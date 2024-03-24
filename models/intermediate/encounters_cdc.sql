{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='delete+insert',
    )
}}

SELECT
    enc.id
    , enc._airbyte_ab_id AS airbyte_raw_id 
    , TO_TIMESTAMP(enc.max_scheduled_date, 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') AS max_scheduled_date
    , TO_TIMESTAMP(enc.last_modified_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') AS last_modified_at
    , enc.subject_external_id
    , enc.audit
    , TO_TIMESTAMP(enc.cancel_date_time, 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') AS cancel_date_time
    , enc.cancel_location
    , TO_TIMESTAMP(enc.earliest_scheduled_date, 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') AS earliest_scheduled_date
    , TO_TIMESTAMP(enc.encounter_date_time, 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') AS encounter_date_time
    , enc.encounter_type
    , enc.encounter_location
    , enc.observations
    , enc.external_id
    , enc.voided
    , enc.cancelobservations
    , enc.subject_type
    , enc.subject_id
    , CASE 
        WHEN TO_TIMESTAMP(enc.last_modified_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') = TO_TIMESTAMP(json_extract_path_text(enc.audit::json, 'Created at'), 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') THEN 'C'
        WHEN TO_TIMESTAMP(enc.last_modified_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') >= TO_TIMESTAMP(json_extract_path_text(enc.audit::json, 'Created at'), 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') THEN 'U'
        -- How should we handle cancellations?
        ELSE 'NA'
    END AS op_type
FROM {{ source('silver', 'encounters_normalized') }} AS enc

{% if is_incremental() %}
  WHERE TO_TIMESTAMP(enc.last_modified_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"T"TZ') >= (SELECT MAX(last_modified_at) FROM {{ this }})
{% endif %}