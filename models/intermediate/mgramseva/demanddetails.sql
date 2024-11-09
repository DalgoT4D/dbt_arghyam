{{ config(
    materialized='table', schema="mgramseva"
) }}

WITH flattened AS (
    SELECT 
        data->>'id' AS demandid,
        data->>'status' AS demandstatus,
        data->>'tenantId' AS tenantid,
        data->>'consumerCode' AS consumercode,
        data->>'taxPeriodTo' AS demandtodate, 
        data->>'taxPeriodFrom' AS demandfromdate,
        data->'auditDetails'->>'lastModifiedTime' AS lastmodifiedate,
        jsonb_array_elements((data->>'demandDetails')::jsonb)->>'id' AS demanddetailid,
        jsonb_array_elements((data->>'demandDetails')::jsonb)->>'taxAmount' AS demandamount
    FROM {{ source('source_mgramseva', 'demands') }}
)

SELECT 
    tenantid, 
    demandid,
    demandstatus,
    consumercode,
    to_timestamp(demandtodate::numeric / 1000) :: date AS demandtodate,
    to_timestamp(demandfromdate::numeric / 1000) :: date AS demandfromdate,
    to_timestamp(lastmodifiedate::numeric / 1000) :: date AS lastmodifiedate,
    demanddetailid, 
    demandamount::numeric AS demandamount
FROM flattened
