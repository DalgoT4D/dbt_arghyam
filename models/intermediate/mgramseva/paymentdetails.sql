{{ config(
    materialized='table', schema="mgramseva"
) }}


with payment_details as (
  select 
		"data"->>'id' as paymentid,
		"data"->>'tenantId' as tenantid,
		("data"->>'totalDue')::numeric as totalpaymentdue,
		("data"->>'totalAmountPaid')::numeric as totalpaymentpaid,
		to_timestamp(
			ROUND(
				("data"->>'transactionDate')::numeric * 0.001
			)
		) AT TIME ZONE 'UTC' AS paymentdate,
		"data"->>'transactionNumber' as paymenttxnnumber,
		"data"->>'mobileNumber' as mobilenumber,
		"data"->>'payerId' as payerid,
    jsonb_array_elements(("data"->>'paymentDetails')::jsonb) as paymentdetail

  from {{ source('source_mgramseva', 'payments') }}
)
select 
	paymentid,
	tenantid,
	totalpaymentdue,
	totalpaymentpaid,
	paymentdate,
	paymenttxnnumber,
	mobilenumber,
	payerid,
	"paymentdetail"->>'id' as paymentdetailid,
	"paymentdetail"->>'billId' as billid,
	"paymentdetail"->'bill'->>'billNumber' as billnumber,
	"paymentdetail"->'bill'->>'consumerCode' as consumercode,
	("paymentdetail"->>'totalDue')::numeric as totalpaymentdetaildue,
	("paymentdetail"->>'totalAmountPaid')::numeric as totalpaymentdetailpaid,
	"paymentdetail"->>'receiptNumber' as receiptnumber,
	to_timestamp(
		ROUND(
			("paymentdetail"->'receiptDate')::numeric * 0.001
		)
	) AT TIME ZONE 'UTC'  as receiptdate
	
from payment_details