{{ config(materialized='table') }}


WITH expense AS (
    SELECT 
        tenantid, 
        TO_CHAR(todate, 'Month') AS month_name,
        todate::date AS date, 
        COALESCE(SUM(totalexpenditure), 0) AS total_expenditure  
    FROM 
        {{ref('tenantexpenses')}} 
    GROUP BY 
        tenantid, 
        TO_CHAR(todate, 'Month'), 
        todate::date  
)

SELECT 
    COALESCE(d.tenantid, e.tenantid) AS tenantid,  
    COALESCE(d.username, 'Unknown') AS username,  
    COALESCE(e.total_expenditure, 0) AS total_expenditure,  
    COALESCE(DATE(d."date"), e.date) AS payment_date, 
    COALESCE(d.reporting_year, EXTRACT(YEAR FROM e.date)) AS reporting_year, 
    COALESCE(d.reporting_month, e.month_name) AS reporting_month,  
    COALESCE(d.total_amount_paid, 0) AS total_amount_paid  
FROM 
    {{ref('demand_collection')}} d
FULL OUTER JOIN 
    expense e
ON 
    d.tenantid = e.tenantid AND
    DATE(d."date") = e.date 
ORDER BY 
    COALESCE(d.tenantid, e.tenantid)  