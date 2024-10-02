-- 1. Configuration: The first line sets the configuration options for the subsequent code. 
--    It specifies that the resulting table should be materialized as a regular table in the 'intermediate_analytics_mgramseva' schema.

-- 2. Common Table Expressions (CTEs): CTEs are temporary result sets that can be referenced within the query. 
--    In this code, there are three CTEs defined: `table_p`, `taple_d`, `final`.

--    - `table_p`: It essentially aggregates the total payment made by each consumer for each tenant on each specific day and month.
     
--    - `table_d`: This query aggregates the total demand amount for each consumer and tenant on a daily and monthly basis.
   
--    - `final`: This query combines payment and demand data for each consumercode and tenantid, after that total amount paid is subtracted from total demand to calculate total advance
      -- , and total demand is subtracted by total amount paid to calculate arrears

-- 3. Final Query: This would result in a final dataset that includes financial amounts (paid and due), total advance , arrears, tenant names, and usernames.

-- In summary, this query combines data from two tables, `paymentdetailes` and `demanddeatils`, using a full outer join based on a common column. 
   -- To give us a table that contains total advance and total arrears for all the consumer codes, tenantids and usernames.

-- Read about left join here ->>>>> https://www.tutorialspoint.com/sql/sql-full-joins.htm

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
           DATE(TO_TIMESTAMP(COALESCE(table_d.date, table_p.date), 'YYYY-MM-DD')) AS date,  -- Changed format to `date`
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
SELECT f.*, 
           COALESCE(u.username, 'No Username') AS username,
           f.date::date AS formatted_date,  -- Casting timestamptz to date
           -- Case statement to derive month_number from reporting_month
           CASE
               WHEN f.reporting_month = 'January  ' THEN '01 - January'
          WHEN f.reporting_month = 'February ' THEN '02 - February'
        WHEN f.reporting_month = 'March    ' THEN '03 - March'
        WHEN f.reporting_month = 'April    ' THEN '04 - April'
        WHEN f.reporting_month = 'May      ' THEN '05 - May'
        WHEN f.reporting_month = 'June     ' THEN '06 -  June'
        WHEN f.reporting_month = 'July     ' THEN '07 - July'
        WHEN f.reporting_month = 'August   ' THEN '08 - August'
        WHEN f.reporting_month = 'September' THEN '09 - September'
        WHEN f.reporting_month = 'October  ' THEN '10 - October'
        WHEN f.reporting_month = 'November ' THEN '11 - November'
        WHEN f.reporting_month = 'December ' THEN '12 - December'
           END AS reporting_month_number
    FROM final AS f 
    LEFT JOIN {{ref('user_tenantid')}} AS u
        ON REGEXP_REPLACE(f.tenantid, '.*br\.', '') = u.tenant_name
    ORDER BY f.tenantid
