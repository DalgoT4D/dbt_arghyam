{{ config(materialized='table') }}

WITH table_p AS (
    SELECT consumercode, 
           tenantid,
           TO_CHAR(paymentdate, 'YYYY-MM-DD') AS date, 
           TO_CHAR(paymentdate, 'Month') AS reporting_month, 
           SUM(totalpaymentpaid) AS amount_p
    FROM {{ref('paymentdetails')}}
    GROUP BY consumercode, tenantid, TO_CHAR(paymentdate, 'YYYY-MM-DD'), TO_CHAR(paymentdate, 'Month')
    ORDER BY consumercode, TO_CHAR(paymentdate, 'YYYY-MM-DD')
),

table_d AS (
    SELECT consumercode, 
           tenantid,
           TO_CHAR(demandtodate, 'YYYY-MM-DD') AS date, 
           TO_CHAR(demandtodate, 'Month') AS reporting_month, 
           SUM(demandamount) AS amount_d
    FROM {{ref('demanddetails')}}
    GROUP BY consumercode, tenantid, TO_CHAR(demandtodate, 'YYYY-MM-DD'), TO_CHAR(demandtodate, 'Month')
    ORDER BY consumercode, TO_CHAR(demandtodate, 'YYYY-MM-DD')
),

water_connections AS (
    SELECT COALESCE(table_d.consumercode, table_p.consumercode) AS consumercode, 
           COALESCE(table_d.tenantid, table_p.tenantid) AS tenantid,
           -- Convert timestampz to date
           TO_TIMESTAMP(COALESCE(table_d.date, table_p.date), 'YYYY-MM-DD')::date AS date, 
           COALESCE(table_d.reporting_month, table_p.reporting_month) AS reporting_month, 
           EXTRACT(YEAR FROM TO_TIMESTAMP(COALESCE(table_d.date, table_p.date), 'YYYY-MM-DD')) AS reporting_year,
           COALESCE(amount_p, 0) AS total_amount_paid, 
           COALESCE(amount_d, 0) AS total_amount_due
    FROM table_p
    FULL OUTER JOIN table_d 
        ON table_p.consumercode = table_d.consumercode 
        AND table_p.date = table_d.date
    ORDER BY consumercode, date
),

-- Join with another table based on consumerno
final AS (
    SELECT wc.*,
           w.status
    FROM water_connections AS wc 
    LEFT JOIN {{ref('waterconnections')}} AS w
        ON wc.consumercode = w.connectionno
    ORDER BY wc.consumercode, wc.date
)

-- Including username and formatting date
SELECT f.*, 
       COALESCE(u.username, 'No Username') AS username,
       f.date::date AS formatted_date  -- Casting timestamptz to date
FROM final AS f 
LEFT JOIN {{ref('user_tenantid')}} AS u
    ON REGEXP_REPLACE(f.tenantid, '.*br\.', '') = u.tenant_name
ORDER BY f.tenantid
