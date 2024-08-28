{{ config(
    materialized='table', schema="mgramseva"
) }}

WITH flattened AS (
  select 
    "data"->>'id' as demandid,
    "data"->>'status' as demandstatus,
    "data"->>'tenantId' as tenantid,
    "data"->>'consumerCode' as consumercode,
    "data"->>'demandToDate' as demandtodate,
    "data"->>'demandFromDate' as demandfromdate,
    jsonb_array_elements(("data"->>'demandDetails')::jsonb)->>'id' as demanddetailid,
    jsonb_array_elements(("data"->>'demandDetails')::jsonb)->>'taxAmount' as demandamount
  from {{ source('source_mgramseva', 'demands') }}
)

SELECT 
  tenantid, 
  demandid,
  demandstatus,
  consumercode,
  demandtodate,
  demandfromdate,
  demanddetailid, 
  demandamount::numeric as demandamount
FROM flattened