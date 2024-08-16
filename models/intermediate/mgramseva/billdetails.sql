{{ config(
    materialized='table', schema="mgramseva"
) }}

WITH flattened AS (
  select 
    "data"->>'tenantId' as tenantid,
    "data"->>'consumerCode' as consumercode,
    "data"->>'id' as billid,
    "data"->>'billNumber' as billnumber,
    to_timestamp(
      ROUND(
        ("data"->>'billDate')::numeric * 0.001
      )
    ) AT TIME ZONE 'UTC' AS billdate,
    ("data"->>'totalAmount')::numeric as totalbillamount,
    "data"->>'mobileNumber' as mobilenumber,
    jsonb_array_elements(("data"->>'billDetails')::jsonb)->>'id' as billdetailid,
    jsonb_array_elements(("data"->>'billDetails')::jsonb)->>'demandId' as demandid,
    jsonb_array_elements(("data"->>'billDetails')::jsonb)->>'toPeriod' as toperiod,
    jsonb_array_elements(("data"->>'billDetails')::jsonb)->>'fromPeriod' as fromperiod,
    jsonb_array_elements(("data"->>'billDetails')::jsonb)->>'amount' as billamount
  from {{ source('source_mgramseva', 'bills') }}
)

select 
  tenantid, 
  consumercode,
  demandid,
  billid, 
  billdetailid, 
  billnumber,
  billdate,
  totalbillamount,
  mobilenumber,
  to_timestamp(
		ROUND(toperiod::numeric * 0.001)
	) AT TIME ZONE 'UTC' AS billtoperiod,
	to_timestamp(
		ROUND(fromperiod::numeric * 0.001)
	) AT TIME ZONE 'UTC' AS billfromperiod, 
  billamount::numeric as billamount
from flattened
