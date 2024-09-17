{{ config(materialized='table') }}
SELECT username.username, username.name, username."LocationHierarchy", tenant_id.tenant_name
  FROM
       {{ source('source_mgramseva_analysis', 'username') }} AS username
  RIGHT OUTER JOIN
       {{ source('source_mgramseva_analysis', 'tenant_id') }} AS tenant_id
  ON username."LocationHierarchy" = tenant_id."LocationHierarchy"