{{ config(
    materialized='table', schema="mgramseva"
) }}

select 
	"data"->>'id' as billid,
	"data"->>'tenantId' as tenantid,
	"data"->>'billNumber' as billnumber,
	to_timestamp(
		ROUND(
			("data"->>'billDate')::numeric * 0.001
		)
	) AT TIME ZONE 'UTC' AS billdate,
	("data"->>'totalAmount')::numeric as totalamount,
	"data"->>'consumerCode' as consumercode,
	"data"->>'mobileNumber' as mobilenumber
from {{ source('source_mgramseva', 'bills') }}
