-- Tank Cleaning in last 6 months
SELECT
    COUNT(*) AS tank_cleaning_count
FROM activity_fact af
JOIN activity_dim ad ON af.activity_id = ad.id
WHERE ad.activity_type = 'Tank Cleaning'
AND af.activity_date >= CURRENT_DATE - INTERVAL '6 months';

-- Water Quality Testing Done in 6 months
SELECT
    COUNT(*) AS water_quality_testing_count
FROM activity_fact af
JOIN activity_dim ad ON af.activity_id = ad.id
WHERE ad.activity_type = 'Water Quality Testing'
AND af.activity_date >= CURRENT_DATE - INTERVAL '6 months';

-- Average days water has been supplied in a quarter
SELECT
    AVG(days_supplied) AS avg_days_supplied
FROM (
    SELECT
        DATE_TRUNC('quarter', supply_date) AS quarter,
        COUNT(*) AS days_supplied
    FROM water_supply_fact
    WHERE supply_status = 'Supplied' -- Assuming a status column exists
    GROUP BY quarter
) AS quarterly_supply
WHERE quarter >= DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months';

-- No. Of Jal Chaupals in the last 3 months
SELECT
    COUNT(*) AS jal_chaupal_count
FROM meeting_form_responses_fact
WHERE activity_type = 'Jal Chaupal'
AND meeting_date >= CURRENT_DATE - INTERVAL '3 months';

-- No. Of WIMCs in the last 3 months
SELECT
    COUNT(*) AS wimc_count
FROM meeting_form_responses_fact
WHERE activity_type = 'WIMC'
AND meeting_date >= CURRENT_DATE - INTERVAL '3 months';

-- Water quality parameters 

SELECT
    'PH' AS parameter,
    MAX(date_testing) AS last_test_done_date,
    MAX(ph_count) AS last_test_value,
    '6.5-8.5' AS permissible_limits
FROM water_quality_testing_form_responses_fact
UNION ALL
SELECT
    'Total Hardness',
    MAX(date_testing),
    MAX(hardness),
    '200-600'
FROM water_quality_testing_form_responses_fact
UNION ALL
SELECT
    'Total Alkalinity',
    MAX(date_testing),
    MAX(total_alkalinity),
    '200-600'
FROM water_quality_testing_form_responses_fact
UNION ALL
SELECT
    'Chloride',
    MAX(date_testing),
    MAX(chloride_count),
    '250-1000'
FROM water_quality_testing_form_responses_fact
UNION ALL
SELECT
    'Nitrate',
    MAX(date_testing),
    MAX(nitrate_count),
    '45'
FROM water_quality_testing_form_responses_fact
UNION ALL
SELECT
    'Arsenic',
    MAX(date_testing),
    MAX(arsenic_count),
    '0.01'
FROM water_quality_testing_form_responses_fact
UNION ALL
SELECT
    'Fluoride',
    MAX(date_testing),
    MAX(fluoride_count),
    '1-1.5'
FROM water_quality_testing_form_responses_fact
UNION ALL
SELECT
    'Iron',
    MAX(date_testing),
    MAX(iron_count),
    '0.3-1'
FROM water_quality_testing_form_responses_fact
UNION ALL
SELECT
    'Bacteriological Contamination',
    MAX(date_testing),
    MAX(bacterial_contamination),
    'No'
FROM water_quality_testing_form_responses_fact;
