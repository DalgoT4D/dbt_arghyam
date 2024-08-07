{{ config(
    materialized='table'
) }}

WITH flattened AS (
  select 
    jsonb_array_elements(("data"->>'demandDetails')::jsonb)->>'tenantId' as tenantid,
    jsonb_array_elements(("data"->>'demandDetails')::jsonb)->>'demandId' as demandid,
    jsonb_array_elements(("data"->>'demandDetails')::jsonb)->>'id' as demanddetailid,
    jsonb_array_elements(("data"->>'demandDetails')::jsonb)->>'taxAmount' as amount
  from {{ source('source_mgramseva', 'demands') }}
)

SELECT 
  tenantid, 
  demandid,
  demanddetailid, 
  amount
FROM flattened