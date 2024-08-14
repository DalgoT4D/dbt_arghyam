{{ config(
    materialized='table', schema="mgramseva"
) }}

select 
  "data"->>'OMMisc' as ommisc,
  "data"->>'tenantId' as tenantid,
  to_date("data"->>'fromDate', 'YYYY-MM-DD') as fromdate,
  to_date("data"->>'toDate', 'YYYY-MM-DD') as todate,
  ("data"->>'salary')::numeric as salary,
  ("data"->>'billsPaid')::numeric as billspaid,
  ("data"->>'amountPaid')::numeric as amountpaid,
  ("data"->>'totalBills')::numeric as totalbills,
  ("data"->>'amountUnpaid')::numeric as amountunpaid,
  ("data"->>'pendingBills')::numeric as pendingbills,
  ("data"->>'electricityBill')::numeric as electricitybill,
  ("data"->>'totalExpenditure')::numeric as totalexpenditure
  
from {{ source('source_mgramseva', 'tenant_expenses') }}

