WITH monthly_activities AS (
    SELECT
        DATE_TRUNC('month', meeting_date) AS month,
        encounter_type,
        COUNT(*) AS count
    FROM
        {{ ref('meeting_form_responses_fact') }}
    WHERE
        meeting_date >= (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '6 months')
        AND meeting_date < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'
    GROUP BY
        DATE_TRUNC('month', meeting_date),
        encounter_type
),

pivoted AS (
    SELECT
        month,
        MAX(CASE WHEN encounter_type = 'WIMC Meeting' THEN 'Yes' ELSE 'No' END) AS wimc_meeting,
        MAX(CASE WHEN encounter_type = 'Jal Chaupal' THEN 'Yes' ELSE 'No' END) AS jal_chaupal,
        MAX(CASE WHEN encounter_type = 'Logbook' THEN 'Yes' ELSE 'No' END) AS logbook
    FROM
        monthly_activities
    GROUP BY
        month
)

SELECT
    TO_CHAR(month, 'Mon') AS month_name,
    wimc_meeting,
    jal_chaupal,
    logbook
FROM
    pivoted
ORDER BY
    month;
