-- 1. Configuration: The first line sets the configuration options for the subsequent code. 
--    It specifies that the resulting table should be materialized as a regular table in the 'intermediate_analytics_mgramseva' schema.

-- 2. Common Table Expressions (CTEs): CTEs are temporary result sets that can be referenced within the query. 
--    In this code, there are five CTEs defined: `water_stats`, `quality_stats`, `wimc_stats`,`jal_chaupal_stats` and `tariff_collection` .

--    - `water_stats`: This CTE calculates the total days each user had water (total_days_with_water) and the total days recorded (water or no water) within the last 180 days. It references log_book_form_responses_fact to get data on water days and non-water days.

--    - `quality_stats`: This CTE finds the most recent date of water quality testing for each user (last_test_date), assessing the recency of quality checks. It references water_quality_testing_form_responses_fact.

--    - `wimc_stats`: This CTE counts the number of WIMC meetings attended by each user in the last 6 months.It uses the wimc_meeting_form_responses_fact table for meeting records.

--    - `jal_chaupal_stats`: This CTE counts the Jal Chaupal meetings attended by each user in the last 6 months.It uses the "jal_chaupal_form_responses_fact" table for these records.

--    - `tariff_collection`: This CTE aggregates tariff collection data, calculating the total amount collected and 
--the target amount due for each user in the last 6 months. We are getting this using the "demand_collection" table

-- 3. Final Query: The final query combines data from each CTE with a LEFT JOIN on username to calculate key scores: percent_days_with_water: % of days with water.
--water_availability_score: 1 for 90%+, 0.5 for 60-89%, otherwise 0.
--water_quality_score: 1 if tested within 6 months, else 0.
--wimc_meeting_score & jal_chaupal_score: 1 for 4+ meetings, 0.5 for 2-4, else 0.
--water_tariff_collection_score: 1 if 50%+ of target collected, 0.5 for 25-49%, else 0.
--total_score: Sum of all individual scores.

-- In summary,  This query aggregates and scores data on water availability, quality, community involvement, and financial 
--contributions for each user over the past 6 months, creating a composite total_score to reflect user engagement and service status.

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
        MAX(meeting_date) AS last_test_date,
        COUNT(DISTINCT meeting_date) AS wq_times
    FROM 
        {{ ref('water_quality_testing_form_responses_fact') }} as qs
    WHERE 
        meeting_date >= CURRENT_DATE - INTERVAL '6 months'
    GROUP BY 
        username
),

wimc_stats AS (
    SELECT
        username,
        COUNT(meeting_date) AS wimc_meeting_count
    FROM 
        {{ ref('wimc_meeting_form_responses_fact') }} as wimc
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
        {{ ref('jal_chaupal_form_responses_fact') }} as jc
    WHERE 
        meeting_date >= CURRENT_DATE - INTERVAL '6 months'
    GROUP BY
        username
),


tank_cleaning AS (
    SELECT
        username,
        COUNT(DISTINCT encounter_id) AS tank_cleaning_count
    FROM 
        {{ ref('tank_cleaning_form_responses_fact') }} AS tc
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
        -- replace with your table name for tariff collection
        {{ ref('demand_collection') }}
    WHERE
        meeting_date >= CURRENT_DATE - INTERVAL '6 months'
    GROUP BY
        username
)

SELECT
    ws.username,
    COALESCE(ws.total_days_with_water,0) as "जल_आपूर्ति_औसत",
    ws.total_days,
    (ws.total_days_with_water::FLOAT / ws.total_days)
    * 100 AS percent_days_with_water,

    CASE
        WHEN (ws.total_days_with_water::FLOAT / ws.total_days) >= 0.9 THEN 1
        WHEN (ws.total_days_with_water::FLOAT / ws.total_days) >= 0.6 THEN 0.5
        ELSE 0
    END AS "जल_उपलब्धता_स्कोर",

    CASE
        WHEN qs.last_test_date >= CURRENT_DATE - INTERVAL '6 months' THEN 1
        ELSE 0
    END AS "जल_गुणवत्ता_स्कोर",
    
    CASE 

        WHEN wimc.wimc_meeting_count > 4 THEN 1
        WHEN wimc.wimc_meeting_count BETWEEN 2 AND 4 THEN 0.5
        ELSE 0
    END AS "wimc_मीटिंग_स्कोर",

    CASE
        WHEN jc.jal_chaupal_count > 4 THEN 1
        WHEN jc.jal_chaupal_count BETWEEN 2 AND 4 THEN 0.5
        ELSE 0
    END AS "जल_चौपाल_स्कोर",

    CASE
        WHEN
            tc.total_collected::FLOAT / NULLIF(tc.total_target, 0) >= 0.5
            THEN 1
        WHEN
            tc.total_collected::FLOAT / NULLIF(tc.total_target, 0) >= 0.25
            THEN 0.5
        ELSE 0
    END AS "जल_टैरिफ_संग्रह_स्कोर",

    (CASE
        WHEN (ws.total_days_with_water::FLOAT / ws.total_days) >= 0.9 THEN 1
        WHEN (ws.total_days_with_water::FLOAT / ws.total_days) >= 0.6 THEN 0.5
        ELSE 0
    END
    + CASE
        WHEN qs.last_test_date >= CURRENT_DATE - INTERVAL '6 months' THEN 1
        ELSE 0
    END
    + CASE
        WHEN wimc.wimc_meeting_count > 4 THEN 1
        WHEN wimc.wimc_meeting_count BETWEEN 2 AND 4 THEN 0.5
        ELSE 0
    END
    + CASE
        WHEN jc.jal_chaupal_count > 4 THEN 1
        WHEN jc.jal_chaupal_count BETWEEN 2 AND 4 THEN 0.5
        ELSE 0
    END
    + CASE
        WHEN
            tc.total_collected::FLOAT / NULLIF(tc.total_target, 0) >= 0.5
            THEN 1
        WHEN
            tc.total_collected::FLOAT / NULLIF(tc.total_target, 0) >= 0.25
            THEN 0.5
        ELSE 0
    END) AS "कुल_स्कोर",

      COALESCE(qs.wq_times,0) AS "जल_गुणवत्ता_परीक्षण",
    COALESCE(wt.tank_cleaning_count,0) AS "टैंक_सफाई_की_संख्या",
    COALESCE(wimc.wimc_meeting_count,0) AS "wimc_बैठक_की_संख्या",
    COALESCE(jc.jal_chaupal_count,0) AS "जल_चौपाल_की_संख्या"

FROM 
    water_stats ws
LEFT JOIN 
    quality_stats qs ON ws.username = qs.username
LEFT JOIN 
    wimc_stats wimc ON ws.username = wimc.username
LEFT JOIN 
    jal_chaupal_stats jc ON ws.username = jc.username
LEFT JOIN 
    tank_cleaning wt ON ws.username = wt.username -- Corrected Join
LEFT JOIN 
    tariff_collection tc ON ws.username = tc.username

