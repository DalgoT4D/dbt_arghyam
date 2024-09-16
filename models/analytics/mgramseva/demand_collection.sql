{{ config(materialized='table') }}

WITH table_p AS (
    SELECT consumercode, 
           tenantid,
           TO_CHAR(paymentdate, 'YYYY-MM-DD') AS month, 
           TO_CHAR(paymentdate, 'Month') AS month_name, 
           SUM(totalpaymentpaid) AS amount_p
    FROM {{ref('paymentdetails')}}
    GROUP BY consumercode, tenantid, TO_CHAR(paymentdate, 'YYYY-MM-DD'), TO_CHAR(paymentdate, 'Month')
    ORDER BY consumercode, TO_CHAR(paymentdate, 'YYYY-MM-DD')
),

table_d AS (
    SELECT consumercode, 
           tenantid,
           TO_CHAR(demandtodate, 'YYYY-MM-DD') AS month, 
           TO_CHAR(demandtodate, 'Month') AS month_name, 
           SUM(demandamount) AS amount_d
    FROM {{ref('demanddetails')}}
    GROUP BY consumercode, tenantid, TO_CHAR(demandtodate, 'YYYY-MM-DD'), TO_CHAR(demandtodate, 'Month')
    ORDER BY consumercode, TO_CHAR(demandtodate, 'YYYY-MM-DD')
),

water_connections AS (
    SELECT COALESCE(table_d.consumercode, table_p.consumercode) AS consumercode, 
           COALESCE(table_d.tenantid, table_p.tenantid) AS tenantid,
           TO_TIMESTAMP(COALESCE(table_d.month, table_p.month), 'YYYY-MM-DD') AS month, 
           TO_TIMESTAMP(COALESCE(table_d.month, table_p.month), 'YYYY-MM') AS onlymonth, 
           COALESCE(table_d.month_name, table_p.month_name) AS month_name, 
           EXTRACT(YEAR FROM TO_TIMESTAMP(COALESCE(table_d.month, table_p.month), 'YYYY-MM-DD')) AS year,
           COALESCE(amount_p, 0) AS total_amount_paid, 
           COALESCE(amount_d, 0) AS total_amount_due
    FROM table_p
    FULL OUTER JOIN table_d 
        ON table_p.consumercode = table_d.consumercode 
        AND table_p.month = table_d.month
    ORDER BY consumercode, month
)

-- Join with another table based on consumerno
SELECT wc.*,
       w.status
FROM water_connections as wc 
LEFT JOIN {{ref('waterconnections')}} as w
    ON wc.consumercode = w.connectionno
ORDER BY wc.consumercode, wc.month
