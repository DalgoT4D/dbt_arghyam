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
        TO_CHAR(paymentdate, 'YYYY-MM-DD') AS meeting_date, 
        TO_CHAR(paymentdate, 'Month') AS reporting_month, 
        SUM(totalpaymentpaid) AS amount_p
    FROM {{ ref('paymentdetails') }}
    GROUP BY consumercode, tenantid, TO_CHAR(paymentdate, 'YYYY-MM-DD'), TO_CHAR(paymentdate, 'Month')
    ORDER BY consumercode, TO_CHAR(paymentdate, 'YYYY-MM-DD')
),

table_d AS (
    SELECT
        consumercode, 
        tenantid,
        TO_CHAR(demandtodate, 'YYYY-MM-DD') AS meeting_date, 
        TO_CHAR(demandtodate, 'Month') AS reporting_month, 
        SUM(demandamount) AS amount_d
    FROM {{ ref('demanddetails') }}
    GROUP BY consumercode, tenantid, TO_CHAR(demandtodate, 'YYYY-MM-DD'), TO_CHAR(demandtodate, 'Month')
    ORDER BY consumercode, TO_CHAR(demandtodate, 'YYYY-MM-DD')
),

water_connections AS (
    SELECT
        TO_TIMESTAMP(COALESCE(table_d.meeting_date, table_p.meeting_date), 'YYYY-MM-DD')::date AS meeting_date, 
        COALESCE(table_d.consumercode, table_p.consumercode) AS consumercode,
        -- Convert timestampz to date
        COALESCE(table_d.tenantid, table_p.tenantid) AS tenantid, 
        COALESCE(table_d.reporting_month, table_p.reporting_month) AS reporting_month, 
        EXTRACT(YEAR FROM TO_TIMESTAMP(COALESCE(table_d.meeting_date, table_p.meeting_date), 'YYYY-MM-DD'))
            AS reporting_year,
        COALESCE(amount_p, 0) AS total_amount_paid, 
        COALESCE(amount_d, 0) AS total_amount_due,
        COALESCE(amount_p-amount_d, 0) AS total_advance,
        COALESCE(amount_d-amount_p, 0) AS total_arrears
    FROM table_p
    FULL OUTER JOIN table_d 
        ON
            table_p.consumercode = table_d.consumercode 
            AND table_p.meeting_date = table_d.meeting_date
    ORDER BY consumercode, meeting_date
),

-- Join with another table based on consumerno
final AS (
    SELECT
        wc.*,
        w.status
    FROM water_connections AS wc 
    LEFT JOIN {{ ref('waterconnections') }} AS w
        ON wc.consumercode = w.connectionno
    ORDER BY wc.consumercode, wc.meeting_date
)

-- Join on the month number
SELECT
    f.*, 
    COALESCE(u.username, 'No Username') AS username,
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
LEFT JOIN {{ ref('transformed_tenantid') }} AS u
    ON f.tenantid = u.tenantid
ORDER BY f.tenantid
