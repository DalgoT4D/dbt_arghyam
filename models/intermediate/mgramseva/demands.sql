{{ config(
    materialized='table', schema="mgramseva"
) }}

select 
	"data"->>'id' as demandid,
	"data"->>'status' as demandstatus,
	"data"->>'tenantId' as tenantid,
	"data"->>'consumerCode' as consumercode,
	to_timestamp(
		ROUND(
			("data"->>'taxPeriodFrom')::numeric * 0.001
		)
	) AT TIME ZONE 'UTC' AS taxperiodfrom,
	to_timestamp(
		ROUND(
			("data"->>'taxPeriodTo')::numeric * 0.001
		)
	) AT TIME ZONE 'UTC' AS taxperiodto

from {{ source('source_mgramseva', 'demands') }}

