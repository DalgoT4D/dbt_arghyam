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

final AS (
    SELECT COALESCE(table_d.consumercode, table_p.consumercode) AS consumercode, 
           COALESCE(table_d.tenantid, table_p.tenantid) AS tenantid,
           TO_TIMESTAMP(COALESCE(table_d.date, table_p.date), 'YYYY-MM-DD') AS date, 
           COALESCE(table_d.reporting_month, table_p.reporting_month) AS reporting_month, 
           EXTRACT(YEAR FROM TO_TIMESTAMP(COALESCE(table_d.date, table_p.date), 'YYYY-MM-DD')) AS reporting_year,
           COALESCE(amount_p, 0) AS total_amount_paid, 
           COALESCE(amount_d, 0) AS total_demand,
           COALESCE(amount_p-amount_d, 0) AS total_advance,
           COALESCE(amount_d-amount_p, 0) AS total_arrears
    FROM table_p
    FULL OUTER JOIN table_d 
        ON table_p.consumercode = table_d.consumercode 
        AND table_p.date = table_d.date
    ORDER BY consumercode, date
)

--including username
select f.*, u.username
from final as f 
left join {{ref('user_tenantid')}} as u
    ON REGEXP_REPLACE(f.tenantid, '.*br\.', '') = u.tenant_name
order by f.tenantid