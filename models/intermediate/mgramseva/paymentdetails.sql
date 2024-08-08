{{ config(
    materialized='table', schema="mgramseva"
) }}


with payment_details as (
  select 
    jsonb_array_elements(("data"->>'paymentDetails')::jsonb) as paymentdetail

  from {{ source('source_mgramseva', 'payments') }}
)
select 
	"paymentdetail"->>'id' as paymentdetailid,
	"paymentdetail"->>'billId' as billid,
	"paymentdetail"->'bill'->>'billNumber' as billnumber,
	"paymentdetail"->'bill'->>'consumerCode' as consumercode,
	"paymentdetail"->'bill'->>'mobileNumber' as mobilenumber,
	("paymentdetail"->>'totalDue')::numeric as totaldue,
	("paymentdetail"->>'totalAmountPaid')::numeric as totalpaid,
	"paymentdetail"->>'receiptNumber' as receiptnumber,
	to_timestamp(
		ROUND(
			("paymentdetail"->'receiptDate')::numeric * 0.001
		)
	) AT TIME ZONE 'UTC'  as receiptdate
	
from payment_details