{{ config(materialized='table') }}

WITH water_stats AS (
    SELECT
        username,
        SUM(days_with_water) AS total_days_with_water,
        SUM(days_with_water + days_no_water) AS total_days
    FROM 
        {{ ref('log_book_form_responses_fact') }}
    WHERE 
        meeting_date >= CURRENT_DATE - INTERVAL '180 days'
    GROUP BY 
        username
),

quality_stats AS (
    SELECT 
        username,
        MAX(meeting_date) AS last_test_date
    FROM 
        {{ ref('water_quality_testing_form_responses_fact') }}
    GROUP BY 
        username
),

wimc_stats AS (
    SELECT
        username,
        COUNT(meeting_date) AS wimc_meeting_count
    FROM 
        {{ ref('wimc_meeting_form_responses_fact') }}
    WHERE 
        meeting_date >= CURRENT_DATE - INTERVAL '6 months'
    GROUP BY 
        username
),

jal_chaupal_stats AS (
    SELECT
        username,
        COUNT(meeting_date) AS jal_chaupal_count
    FROM 
        {{ ref('jal_chaupal_form_responses_fact') }}
    WHERE 
        meeting_date >= CURRENT_DATE - INTERVAL '6 months'
    GROUP BY 
        username
),

tariff_collection AS (
    SELECT
        username,
        SUM(total_amount_paid) AS total_collected,
        SUM(total_amount_due) AS total_target
    FROM 
         {{ ref('demand_collection') }} -- replace with your table name for tariff collection
    WHERE 
        meeting_date >= CURRENT_DATE - INTERVAL '6 months'
    GROUP BY 
        username
)

SELECT 
    ws.username,
    ws.total_days_with_water,
    ws.total_days,
    (ws.total_days_with_water::FLOAT / ws.total_days) * 100 AS percent_days_with_water,
    
    CASE 
        WHEN (ws.total_days_with_water::FLOAT / ws.total_days) >= 0.9 THEN 1
        WHEN (ws.total_days_with_water::FLOAT / ws.total_days) >= 0.6 THEN 0.5
        ELSE 0
    END AS water_availability_score,

    CASE 
        WHEN qs.last_test_date >= CURRENT_DATE - INTERVAL '6 months' THEN 1
        ELSE 0
    END AS water_quality_score,
    
    CASE 
        WHEN wimc.wimc_meeting_count > 4 THEN 1
        WHEN wimc.wimc_meeting_count BETWEEN 2 AND 4 THEN 0.5
        ELSE 0
    END AS wimc_meeting_score,

    CASE 
        WHEN jc.jal_chaupal_count > 4 THEN 1
        WHEN jc.jal_chaupal_count BETWEEN 2 AND 4 THEN 0.5
        ELSE 0
    END AS jal_chaupal_score,

    CASE 
        WHEN tc.total_collected::FLOAT / NULLIF(tc.total_target, 0) >= 0.5 THEN 1
        WHEN tc.total_collected::FLOAT / NULLIF(tc.total_target, 0) >= 0.25 THEN 0.5
        ELSE 0
    END AS water_tariff_collection_score,

     (CASE 
        WHEN (ws.total_days_with_water::FLOAT / ws.total_days) >= 0.9 THEN 1
        WHEN (ws.total_days_with_water::FLOAT / ws.total_days) >= 0.6 THEN 0.5
        ELSE 0
    END +
    CASE 
        WHEN qs.last_test_date >= CURRENT_DATE - INTERVAL '6 months' THEN 1
        ELSE 0
    END +
    CASE 
        WHEN wimc.wimc_meeting_count > 4 THEN 1
        WHEN wimc.wimc_meeting_count BETWEEN 2 AND 4 THEN 0.5
        ELSE 0
    END +
    CASE 
        WHEN jc.jal_chaupal_count > 4 THEN 1
        WHEN jc.jal_chaupal_count BETWEEN 2 AND 4 THEN 0.5
        ELSE 0
    END +
    CASE 
        WHEN tc.total_collected::FLOAT / NULLIF(tc.total_target, 0) >= 0.5 THEN 1
        WHEN tc.total_collected::FLOAT / NULLIF(tc.total_target, 0) >= 0.25 THEN 0.5
        ELSE 0
    END) AS total_score

FROM 
    water_stats ws
LEFT JOIN 
    quality_stats qs ON ws.username = qs.username
LEFT JOIN 
    wimc_stats wimc ON ws.username = wimc.username
LEFT JOIN 
    jal_chaupal_stats jc ON ws.username = jc.username
LEFT JOIN 
    tariff_collection tc ON ws.username = tc.username