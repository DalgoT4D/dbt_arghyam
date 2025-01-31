{{ config(materialized='table') }}

WITH ward_master AS (
    SELECT 
        LOWER("AVNI Username") AS avni_username,
        "Username" AS name, 
        "Mobile Numbers" AS mobile_numbers,
        TRIM(LOWER(SPLIT_PART("Catchement( Dist, Block, GP, Ward)", ',', 1))) AS district_name,
        TRIM(LOWER(SPLIT_PART("Catchement( Dist, Block, GP, Ward)", ',', 2))) AS block_name,
        TRIM(LOWER(SPLIT_PART("Catchement( Dist, Block, GP, Ward)", ',', 3))) AS gp_name,
        TRIM(LOWER(SPLIT_PART("Catchement( Dist, Block, GP, Ward)", ',', 4))) AS ward_name
    FROM prod_intermediate_seeds.ward_master_list
),

tenant_data AS (
    SELECT 
        LOWER(username) AS username, 
        tenantid, 
        ward_code,
        district_name,
        block_name,
        gp_name,
        ward_name
    FROM {{ ref('transformed_tenantid') }}
),

final_table AS (
    SELECT 
        wm.avni_username,
        wm.name,
        wm.mobile_numbers,
        wm.district_name AS master_district_name,
        wm.block_name AS master_block_name,
        wm.gp_name AS master_gp_name,
        wm.ward_name AS master_ward_name,
        td.username,
        td.tenantid,
        td.ward_code,
        td.district_name AS tenantdata_district_name,
        td.block_name AS tenantdata_block_name,
        td.gp_name AS tenantdata_gp_name,
        td.ward_name AS tenantdata_ward_name,
        CASE 
            WHEN td.username IS NOT NULL THEN 'YES'
            ELSE 'NO'
        END AS data_received
    FROM ward_master wm
    LEFT JOIN tenant_data td
    ON wm.avni_username = td.username
)

SELECT * FROM final_table