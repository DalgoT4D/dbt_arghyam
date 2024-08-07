select 
  "data"->>'OMMisc' as ommisc,
  "data"->>'salary' as salary,
  "data"->>'billsPaid' as billspaid,
  "data"->>'amountPaid' as amountpaid,
  "data"->>'totalBills' as totalbills,
  "data"->>'amountUnpaid' as amountunpaid,
  "data"->>'pendingBills' as pendingbills,
  "data"->>'electricityBill' as electricitybill,
  "data"->>'totalExpenditure' as totalexpenditure
  
from {{ source('source_mgramseva', 'tenant_expenses') }}

