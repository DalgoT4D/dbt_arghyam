-- 1. Configuration: The first line sets the configuration options for the subsequent code. 
--    It specifies that the resulting table should be materialized as a regular table in the 'intermediate_analytics_mgramseva' schema.

-- 2. Common Table Expressions (CTEs): CTEs are temporary result sets that can be referenced within the query. 
--    In this code, there are four CTEs defined: `table_p`, `taple_d`, `water_connections` and `final`.

--    - `table_p`: It essentially aggregates the total payment made by each consumer for each tenant on each specific day and month.

--    - `table_d`: This query aggregates the total demand amount for each consumer and tenant on a daily and monthly basis.

--    - `water_connection`: This query combines payment and demand data for each consumercode and tenantid, ensuring all records are included even if they don't have corresponding entries in both tables.

--    - `final`: This query links the water connection data from water_connections with additional metadata (status) from the waterconnections table, combining and returning data based on the consumercode.

-- 3. Final Query: This would result in a final dataset that includes water connection details, financial amounts (paid and due), tenant names, and usernames. Usernames are attached using the table "user_tenantid" 
-- to the table created by `final` CTE using LEFT JOIN.

-- In summary, this query combines data from two tables, `paymentdetailes` and `demanddeatils`, using a full outer join based on a common column. 
-- After we get a table with the above combinations, we add few more details such as status and username using other tables.

-- Read about left join here ->>>>> https://www.tutorialspoint.com/sql/sql-full-joins.htm


{{ config(materialized='table') }}

WITH table_p AS (
    SELECT 
        consumercode, 
        tenantid,
        TO_CHAR(paymentdate - INTERVAL '1 month', 'FMMonth') AS reporting_month, 
        EXTRACT(YEAR FROM paymentdate - INTERVAL '1 month') AS reporting_year,
        MIN(paymentdate - INTERVAL '1 month')::DATE AS meeting_date, -- Select the earliest meeting date
        SUM(totalpaymentpaid) AS amount_p
    FROM {{ ref('paymentdetails') }}
    GROUP BY
        consumercode, tenantid, 
        TO_CHAR(paymentdate - INTERVAL '1 month', 'FMMonth'), 
        EXTRACT(YEAR FROM paymentdate - INTERVAL '1 month')
),

table_d AS (
    SELECT 
        consumercode, 
        tenantid,
        TO_CHAR(demandtodate, 'FMMonth') AS reporting_month, 
        EXTRACT(YEAR FROM demandtodate) AS reporting_year,
        MIN(demandtodate)::DATE AS meeting_date, -- Select the earliest meeting date
        SUM(demandamount) AS amount_d
    FROM {{ ref('demanddetails') }}
    GROUP BY
        consumercode, tenantid, 
        TO_CHAR(demandtodate, 'FMMonth'), 
        EXTRACT(YEAR FROM demandtodate)
),

water_connections AS (
    SELECT 
        COALESCE(table_d.consumercode, table_p.consumercode) AS consumercode, 
        COALESCE(table_d.tenantid, table_p.tenantid) AS tenantid,
        COALESCE(table_d.meeting_date, table_p.meeting_date) AS meeting_date, 
        COALESCE(table_d.reporting_month, table_p.reporting_month) AS reporting_month, 
        COALESCE(table_d.reporting_year, table_p.reporting_year) AS reporting_year,
        COALESCE(amount_p, 0) AS total_amount_paid, 
        COALESCE(amount_d, 0) AS total_amount_due,
        COALESCE(amount_p, 0) - COALESCE(amount_d, 0) AS total_advance,
        COALESCE(amount_d, 0) - COALESCE(amount_p, 0) AS total_arrears
    FROM table_p
    FULL OUTER JOIN table_d 
        ON
            table_p.consumercode = table_d.consumercode 
            AND table_p.reporting_month = table_d.reporting_month
            AND table_p.reporting_year = table_d.reporting_year
),

final AS (
    SELECT 
        wc.*,
        w.status
    FROM water_connections AS wc 
    LEFT JOIN {{ ref('waterconnections') }} AS w
        ON wc.consumercode = w.connectionno
)

SELECT 
    f.tenantid, -- Explicit qualification
    f.consumercode,
    MIN(f.meeting_date) AS meeting_date, -- Select one meeting date per month
    f.reporting_month,
    f.reporting_year,
    f.status,
    COALESCE(f.total_amount_paid, 0) AS total_amount_paid,
    COALESCE(f.total_amount_due, 0) AS total_amount_due,
    COALESCE(f.total_advance, 0) AS total_advance,
    COALESCE(f.total_arrears, 0) AS total_arrears,
    COALESCE(u.username, 'No Username') AS username,
    TO_CHAR(TO_DATE(f.reporting_month, 'FMMonth'), 'MM - Month') AS "माह"
FROM final AS f
LEFT JOIN {{ ref('transformed_tenantid') }} AS u
    ON f.tenantid = u.tenantid
GROUP BY
    f.tenantid, f.consumercode, f.reporting_month, f.reporting_year, 
    f.total_amount_paid, f.total_amount_due, f.total_advance, f.total_arrears, 
    u.username,f.status
ORDER BY f.tenantid, "माह"
