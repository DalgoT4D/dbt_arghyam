{{ config(
    materialized='table', schema="mgramseva"
) }}

WITH flattened AS (
    SELECT
        (data ->> 'totalAmount')::numeric AS totalbillamount,
        data ->> 'tenantId' AS tenantid,
        data ->> 'consumerCode' AS consumercode,
        data ->> 'id' AS billid,
        data ->> 'billNumber' AS billnumber,
        to_timestamp(
            round(
                (data ->> 'billDate')::numeric * 0.001
            )
        ) AT TIME ZONE 'UTC' AS billdate,
        data ->> 'mobileNumber' AS mobilenumber,
        jsonb_array_elements((data ->> 'billDetails')::jsonb)
        ->> 'id' AS billdetailid,
        jsonb_array_elements((data ->> 'billDetails')::jsonb)
        ->> 'demandId' AS demandid,
        jsonb_array_elements((data ->> 'billDetails')::jsonb)
        ->> 'toPeriod' AS toperiod,
        jsonb_array_elements((data ->> 'billDetails')::jsonb)
        ->> 'fromPeriod' AS fromperiod,
        jsonb_array_elements((data ->> 'billDetails')::jsonb)
        ->> 'amount' AS billamount
    FROM {{ source('source_mgramseva', 'bills') }}
)

SELECT
    tenantid,
    consumercode,
    demandid,
    billid,
    billdetailid,
    billnumber,
    billdate,
    totalbillamount,
    mobilenumber,
    billamount::numeric AS billamount,
    to_timestamp(
        round(toperiod::numeric * 0.001)
    ) AT TIME ZONE 'UTC' AS billtoperiod,
    to_timestamp(
        round(fromperiod::numeric * 0.001)
    ) AT TIME ZONE 'UTC' AS billfromperiod
FROM flattened
