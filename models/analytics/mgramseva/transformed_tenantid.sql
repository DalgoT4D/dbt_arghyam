{{ config(materialized='table') }}

with ward as (SELECT log.ward_name,log.district_name,log.block_name,log.gp_name,log.username,
    'w' || 
    CASE SUBSTRING(ward_name, 2, 1) -- get the first digit after 'W'
        WHEN '0' THEN 'a'
        WHEN '1' THEN 'b'
        WHEN '2' THEN 'c'
        WHEN '3' THEN 'd'
        WHEN '4' THEN 'e'
        WHEN '5' THEN 'f'
        WHEN '6' THEN 'g'
        WHEN '7' THEN 'h'
        WHEN '8' THEN 'i'
        WHEN '9' THEN 'j'
        ELSE SUBSTRING(ward_name, 2, 1) -- fallback to original if no match
    END || 
    CASE SUBSTRING(ward_name, 3, 1) -- get the second digit after 'W'
        WHEN '0' THEN 'a'
        WHEN '1' THEN 'b'
        WHEN '2' THEN 'c'
        WHEN '3' THEN 'd'
        WHEN '4' THEN 'e'
        WHEN '5' THEN 'f'
        WHEN '6' THEN 'g'
        WHEN '7' THEN 'h'
        WHEN '8' THEN 'i'
        WHEN '9' THEN 'j'
        ELSE SUBSTRING(ward_name, 3, 1) -- fallback to original if no match
    END AS ward_code
FROM 
      {{ref('log_book_form_responses_fact')}}  as log)
    
SELECT w.district_name,w.block_name,w.gp_name,w.ward_name,w.ward_code,
    'br.'|| LOWER(district_name) || LOWER(block_name) || LOWER(REPLACE(gp_name, ' ', '')) ||ward_code as tenantid,w.username
FROM 
    ward as w


