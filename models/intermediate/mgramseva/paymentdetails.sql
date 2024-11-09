{{ config(
    materialized='table', schema="mgramseva"
) }}


with payment_details as (
    select
        (data ->> 'totalDue')::numeric as totalpaymentdue,
        (data ->> 'totalAmountPaid')::numeric as totalpaymentpaid,
        data ->> 'id' as paymentid,
        data ->> 'tenantId' as tenantid,
        to_timestamp(
            round(
                (data ->> 'transactionDate')::numeric * 0.001
            )
        ) at time zone 'UTC' as paymentdate,
        to_timestamp(
            round(
                (data -> 'auditDetails' ->> 'lastModifiedTime')::numeric * 0.001
            )
        ) at time zone 'UTC' as lastmodifiedate,
        data ->> 'transactionNumber' as paymenttxnnumber,
        data ->> 'mobileNumber' as mobilenumber,
        data ->> 'payerId' as payerid,
        jsonb_array_elements(
            (data ->> 'paymentDetails')::jsonb
        ) as paymentdetail

    from {{ source('source_mgramseva', 'payments') }}
)

select
    paymentid,
    tenantid,
    totalpaymentdue,
    totalpaymentpaid,
    paymentdate,
    lastmodifiedate,
    paymenttxnnumber,
    mobilenumber,
    payerid,
    (paymentdetail ->> 'totalDue')::numeric as totalpaymentdetaildue,
    (paymentdetail ->> 'totalAmountPaid')::numeric as totalpaymentdetailpaid,
    paymentdetail ->> 'id' as paymentdetailid,
    paymentdetail ->> 'billId' as billid,
    paymentdetail -> 'bill' ->> 'billNumber' as billnumber,
    paymentdetail -> 'bill' ->> 'consumerCode' as consumercode,
    paymentdetail ->> 'receiptNumber' as receiptnumber,
    to_timestamp(
        round(
            (paymentdetail -> 'receiptDate')::numeric * 0.001
        )
    ) at time zone 'UTC' as receiptdate

from payment_details
