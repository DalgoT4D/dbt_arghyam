-- 1. Configuration: The first line sets the configuration options for the subsequent code. 
--    It specifies that the resulting table should be materialized as a regular table in the 'intermediate_analytics_mgramseva' schema.

-- 2. Common Table Expressions (CTEs): CTEs are temporary result sets that can be referenced within the query. 
--    In this code, there is just one CTEs defined: `expense`.

--    - `expenditure`: This CTE (expense) aggregates the total expenditure for each tenant (tenantid) by date and month.
     
-- 3. Final Query: This query joins data from the "demand_collection" and expense tables based on tenant ID and date using a full outer join

-- In summary, From this query we get a table that has combined data from the tables "demand_collection" and "tenantexpenses", It shows data of the total amount spent 
   --  by tenants on bills along with the month, year , total amount collected(total amount paid) and usernames associated with those tenantids.

-- Read about full outer join here ->>>>> https://www.tutorialspoint.com/sql/sql-full-joins.htm


{{ config(materialized='table') }}

WITH expense AS (
    SELECT 
        tenantid, 
        TO_CHAR(todate, 'Month') AS month_name,
        todate::date AS meeting_date, 
        COALESCE(SUM(totalexpenditure), 0) AS total_expenditure  
    FROM 
        {{ref('tenantexpenses')}} 
    GROUP BY 
        tenantid, 
        TO_CHAR(todate, 'Month'), 
        todate::date 
),

final AS (
    SELECT 
        COALESCE(d.tenantid, e.tenantid) AS tenantid,  
        COALESCE(d.username, 'Unknown') AS username,  
        COALESCE(e.total_expenditure, 0) AS total_expenditure,  
        COALESCE(DATE(d."meeting_date"), e.meeting_date) AS meeting_date, 
        COALESCE(d.reporting_year, EXTRACT(YEAR FROM e.meeting_date)) AS reporting_year, 
        COALESCE(d.reporting_month, e.month_name) AS reporting_month,
        CASE
            WHEN COALESCE(d.reporting_month, e.month_name) = 'January  ' THEN '01 - January'
            WHEN COALESCE(d.reporting_month, e.month_name) = 'February ' THEN '02 - February'
            WHEN COALESCE(d.reporting_month, e.month_name) = 'March    ' THEN '03 - March'
            WHEN COALESCE(d.reporting_month, e.month_name) = 'April    ' THEN '04 - April'
            WHEN COALESCE(d.reporting_month, e.month_name) = 'May      ' THEN '05 - May'
            WHEN COALESCE(d.reporting_month, e.month_name) = 'June     ' THEN '06 - June'
            WHEN COALESCE(d.reporting_month, e.month_name) = 'July     ' THEN '07 - July'
            WHEN COALESCE(d.reporting_month, e.month_name) = 'August   ' THEN '08 - August'
            WHEN COALESCE(d.reporting_month, e.month_name) = 'September' THEN '09 - September'
            WHEN COALESCE(d.reporting_month, e.month_name) = 'October  ' THEN '10 - October'
            WHEN COALESCE(d.reporting_month, e.month_name) = 'November ' THEN '11 - November'
            WHEN COALESCE(d.reporting_month, e.month_name) = 'December ' THEN '12 - December'
            ELSE 'Unknown'
        END AS reporting_month_number,
        COALESCE(d.total_amount_paid, 0) AS total_amount_paid  
    FROM 
        {{ref('demand_collection')}} d
    FULL OUTER JOIN 
        expense e
    ON 
        d.tenantid = e.tenantid AND
        DATE(d."meeting_date") = e.meeting_date
)

SELECT 
        tenantid,
        username,
        reporting_month_number AS "माह",
        reporting_year AS "वर्ष",
        MAX(total_expenditure) AS totaldb_expenditure,
        SUM(total_amount_paid) AS total_amount_paid
    FROM 
        final
    GROUP BY 
        tenantid, username, reporting_month_number, reporting_year

)
final AS (
    SELECT 
        COALESCE(d.tenantid, e.tenantid) AS tenantid,  
        COALESCE(d.username, 'Unknown') AS username,  
        COALESCE(e.total_expenditure, 0) AS total_expenditure,  
        COALESCE(DATE(d."meeting_date"), e.meeting_date) AS meeting_date, 
        COALESCE(d.reporting_year, EXTRACT(YEAR FROM e.meeting_date)) AS reporting_year, 
        COALESCE(d.reporting_month, e.month_name) AS reporting_month,
        CASE
            WHEN COALESCE(d.reporting_month, e.month_name) = 'January  ' THEN '01 - January'
            WHEN COALESCE(d.reporting_month, e.month_name) = 'February ' THEN '02 - February'
            WHEN COALESCE(d.reporting_month, e.month_name) = 'March    ' THEN '03 - March'
            WHEN COALESCE(d.reporting_month, e.month_name) = 'April    ' THEN '04 - April'
            WHEN COALESCE(d.reporting_month, e.month_name) = 'May      ' THEN '05 - May'
            WHEN COALESCE(d.reporting_month, e.month_name) = 'June     ' THEN '06 - June'
            WHEN COALESCE(d.reporting_month, e.month_name) = 'July     ' THEN '07 - July'
            WHEN COALESCE(d.reporting_month, e.month_name) = 'August   ' THEN '08 - August'
            WHEN COALESCE(d.reporting_month, e.month_name) = 'September' THEN '09 - September'
            WHEN COALESCE(d.reporting_month, e.month_name) = 'October  ' THEN '10 - October'
            WHEN COALESCE(d.reporting_month, e.month_name) = 'November ' THEN '11 - November'
            WHEN COALESCE(d.reporting_month, e.month_name) = 'December ' THEN '12 - December'
            ELSE 'Unknown'
        END AS reporting_month_number,
        COALESCE(d.total_amount_paid, 0) AS total_amount_paid  
    FROM 
        {{ref('demand_collection')}} d
    FULL OUTER JOIN 
        expense e
    ON 
        d.tenantid = e.tenantid AND
        DATE(d."meeting_date") = e.meeting_date
)

SELECT 
        tenantid,
        username,
        reporting_month_number AS "माह",
        reporting_year AS "वर्ष",
        MAX(total_expenditure) AS total_expenditure,
        SUM(total_amount_paid) AS total_amount_paid
    FROM 
        final
    GROUP BY 
        tenantid, username, reporting_month_number, reporting_year

