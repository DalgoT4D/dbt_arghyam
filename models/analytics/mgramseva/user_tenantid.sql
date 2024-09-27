-- Configuration: The first line sets the configuration options for the subsequent code. 
--    It specifies that the resulting table should be materialized as a regular table in the 'intermediate_analytics_mgramseva' schema.

-- In summary, this query combines data from two tables, `username` and `tenant_id`, using a right outer join based on a common column. 
   -- To give us a table that contains usernames for all the tenantids.
   
{{ config(materialized='table') }}
SELECT username.username, username.name, username."LocationHierarchy", tenant_id.tenant_name
  FROM
       {{ source('source_mgramseva_analysis', 'username') }} AS username
  RIGHT OUTER JOIN
       {{ source('source_mgramseva_analysis', 'tenant_id') }} AS tenant_id
  ON username."LocationHierarchy" = tenant_id."LocationHierarchy"