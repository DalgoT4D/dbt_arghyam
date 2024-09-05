{{ config(
    materialized='table', schema="mgramseva"
) }}

WITH flattened AS (
  select 
    "data"->>'id' as demandid,
    "data"->>'status' as demandstatus,
    "data"->>'tenantId' as tenantid,
    "data"->>'consumerCode' as consumercode,
    "data"->>'taxPeriodTo' as demandtodate, 
    "data"->>'taxPeriodFrom' as demandfromdate,
    jsonb_array_elements(("data"->>'demandDetails')::jsonb)->>'id' as demanddetailid,
    jsonb_array_elements(("data"->>'demandDetails')::jsonb)->>'taxAmount' as demandamount
  from {{ source('source_mgramseva', 'demands') }}
)

SELECT 
  tenantid, 
  demandid,
  demandstatus,
  consumercode,
  TO_TIMESTAMP(CAST(demandtodate AS NUMERIC) / 1000) :: DATE as demandToDate,
  TO_TIMESTAMP(CAST(demandfromdate AS NUMERIC) / 1000) :: DATE as demandFromDate,
  demanddetailid, 
  demandamount::numeric as demandamount
FROM flattened
