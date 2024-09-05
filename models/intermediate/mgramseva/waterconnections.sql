{{ config(
    materialized='table', schema="mgramseva"
) }}


with water_connections as (
  select 
    "data"->>'id' as id,
    "data"->>'status' as status,
    "data"->>'advance' as advance,
    "data"->>'arrears' as arrears,
    "data"->>'penalty' as penalty,
    "data"->>'tenantId' as tenantid,
    "data"->>'propertyId' as propertyid,
    "data"->>'connectionNo' as connectionno,
    "data"->>'connectionType' as connectiontype,
    "data"->>'applicationNo' as applicationno,
    jsonb_array_elements(("data"->>'connectionHolders')::jsonb) as connectionholder
  from {{ source('source_mgramseva', 'water_connections') }}
)
select 
  id,
	status,
  coalesce(advance::numeric, 0) as advance,
  coalesce(arrears::numeric, 0) as arrears,
  coalesce(penalty::numeric, 0) as penalty,
	tenantid,
	propertyid,
	connectionno,
	connectiontype,
	applicationno,
	"connectionholder"->>'uuid' as connectionholderuuid,
	"connectionholder"->>'name' as connectionholdername,
	"connectionholder"->>'gender' as connectionholdergender,
	"connectionholder"->>'mobileNumber' as connectionholdermobilenumber
	
from water_connections


