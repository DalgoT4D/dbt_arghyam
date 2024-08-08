{{ config(
    materialized='table', schema="mgramseva"
) }}

select 
  "data"->>'OMMisc' as ommisc,
  "data"->>'tenantId' as tenantid,
  to_timestamp(
		ROUND(
			("data"->'fromDate')::numeric * 0.001
		)
	) AT TIME ZONE 'UTC'  as fromdate,
  to_timestamp(
		ROUND(
			("data"->'toDate')::numeric * 0.001
		)
	) AT TIME ZONE 'UTC'  as todate,
  ("data"->>'salary')::numeric as salary,
  ("data"->>'billsPaid')::numeric as billspaid,
  ("data"->>'amountPaid')::numeric as amountpaid,
  ("data"->>'totalBills')::numeric as totalbills,
  ("data"->>'amountUnpaid')::numeric as amountunpaid,
  ("data"->>'pendingBills')::numeric as pendingbills,
  ("data"->>'electricityBill')::numeric as electricitybill,
  ("data"->>'totalExpenditure')::numeric as totalexpenditure
  
from {{ source('source_mgramseva', 'tenant_expenses') }}

