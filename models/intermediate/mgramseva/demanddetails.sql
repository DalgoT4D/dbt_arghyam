{{ config(
    materialized='table', schema="mgramseva"
) }}

WITH flattened AS (
  select 
    "data"->>'id' as demandid,
    "data"->>'status' as demandstatus,
    "data"->>'tenantId' as tenantid,
    "data"->>'consumerCode' as consumercode,
    to_timestamp(
      ROUND(
        ("data"->'auditDetails'->>'createdTime')::numeric * 0.001
      )
    ) AT TIME ZONE 'UTC' AS demandcreatedtime,

    jsonb_array_elements(("data"->>'demandDetails')::jsonb)->>'id' as demanddetailid,
    jsonb_array_elements(("data"->>'demandDetails')::jsonb)->>'taxAmount' as demandamount
  from {{ source('source_mgramseva', 'demands') }}
)

SELECT 
  tenantid, 
  demandid,
  demandstatus,
  consumercode,
  demandcreatedtime,
  demanddetailid, 
  demandamount::numeric as demandamount
FROM flattened