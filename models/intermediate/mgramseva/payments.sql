{{ config(
    materialized='table'
) }}

select 
	"data"->>'id' as paymentid,
	"data"->>'tenantId' as tenantid,
	("data"->>'totalDue')::numeric as totaldue,
	("data"->>'totalAmountPaid')::numeric as totalpaid,
	to_timestamp(
		ROUND(
			("data"->>'transactionDate')::numeric * 0.001
		)
	) AT TIME ZONE 'UTC' AS transactiondate,
	"data"->>'transactionNumber' as transactionnumber,
	"data"->>'mobileNumber' as mobilenumber,
	"data"->>'payerId' as payerid

from {{ source('source_mgramseva', 'payments') }}

