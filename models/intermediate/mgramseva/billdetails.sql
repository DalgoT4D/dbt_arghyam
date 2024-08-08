{{ config(
    materialized='table', schema="mgramseva"
) }}

WITH flattened AS (
  select 
    jsonb_array_elements(("data"->>'billDetails')::jsonb)->>'tenantId' as tenantid,
    jsonb_array_elements(("data"->>'billDetails')::jsonb)->>'demandId' as demandid,
    jsonb_array_elements(("data"->>'billDetails')::jsonb)->>'billId' as billid,
    jsonb_array_elements(("data"->>'billDetails')::jsonb)->>'id' as billdetailid,
    jsonb_array_elements(("data"->>'billDetails')::jsonb)->>'toPeriod' as toperiod,
    jsonb_array_elements(("data"->>'billDetails')::jsonb)->>'fromPeriod' as fromperiod,
    jsonb_array_elements(("data"->>'billDetails')::jsonb)->>'amount' as amount
  from {{ source('source_mgramseva', 'bills') }}
)

select 
  tenantid, 
  demandid,
  billid, 
  billdetailid, 
  to_timestamp(
		ROUND(toperiod::numeric * 0.001)
	) AT TIME ZONE 'UTC' AS toperiod,
	to_timestamp(
		ROUND(fromperiod::numeric * 0.001)
	) AT TIME ZONE 'UTC' AS fromperiod, 
  amount::numeric as amount
from flattened
