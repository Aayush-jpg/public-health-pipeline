-- =============================================================================
-- Public Health Data Pipeline — Analytics Layer
-- Advanced SQL queries using window functions, CTEs, and aggregations
-- Author: Ayush Ghimire
-- =============================================================================


-- =============================================================================
-- 1. COVID DEATHS ANALYSIS
-- =============================================================================

-- 1a. Total COVID deaths by state, ranked
SELECT
    state,
    SUM(covid_19_deaths)                                    AS total_deaths,
    RANK() OVER (ORDER BY SUM(covid_19_deaths) DESC)        AS death_rank,
    ROUND(
        100.0 * SUM(covid_19_deaths) /
        SUM(SUM(covid_19_deaths)) OVER (), 2
    )                                                        AS pct_of_total
FROM raw_covid_deaths
WHERE covid_19_deaths IS NOT NULL
  AND state NOT IN ('United States', 'Puerto Rico', 'New York City')
GROUP BY state
ORDER BY total_deaths DESC;


-- 1b. Deaths by age group with running total
SELECT
    age_group,
    SUM(covid_19_deaths)                                        AS deaths,
    SUM(SUM(covid_19_deaths)) OVER (
        ORDER BY SUM(covid_19_deaths) DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                                           AS running_total,
    ROUND(
        100.0 * SUM(covid_19_deaths) /
        SUM(SUM(covid_19_deaths)) OVER (), 2
    )                                                           AS pct_share
FROM raw_covid_deaths
WHERE covid_19_deaths IS NOT NULL
  AND age_group NOT IN ('All Ages', 'Not stated')
GROUP BY age_group
ORDER BY deaths DESC;


-- 1c. Weekly death trend with 4-week moving average
WITH weekly AS (
    SELECT
        end_week,
        SUM(covid_19_deaths) AS weekly_deaths
    FROM raw_covid_deaths
    WHERE covid_19_deaths IS NOT NULL
      AND end_week IS NOT NULL
    GROUP BY end_week
)
SELECT
    end_week,
    weekly_deaths,
    ROUND(AVG(weekly_deaths) OVER (
        ORDER BY end_week
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ), 0)                                       AS moving_avg_4wk,
    LAG(weekly_deaths, 1) OVER (ORDER BY end_week) AS prev_week,
    weekly_deaths - LAG(weekly_deaths, 1)
        OVER (ORDER BY end_week)                AS week_over_week_change
FROM weekly
ORDER BY end_week;


-- 1d. Top 3 conditions contributing to COVID deaths by age group
WITH ranked_conditions AS (
    SELECT
        age_group,
        condition,
        SUM(covid_19_deaths)                                AS deaths,
        RANK() OVER (
            PARTITION BY age_group
            ORDER BY SUM(covid_19_deaths) DESC
        )                                                   AS rnk
    FROM raw_covid_deaths
    WHERE covid_19_deaths IS NOT NULL
      AND condition IS NOT NULL
      AND age_group NOT IN ('All Ages', 'Not stated')
    GROUP BY age_group, condition
)
SELECT age_group, condition, deaths, rnk
FROM ranked_conditions
WHERE rnk <= 3
ORDER BY age_group, rnk;


-- =============================================================================
-- 2. CHRONIC DISEASE ANALYSIS
-- =============================================================================

-- 2a. Chronic disease prevalence by state with percentile ranking
WITH state_avg AS (
    SELECT
        location_desc                               AS state,
        category,
        AVG(data_value)                             AS avg_prevalence
    FROM raw_chronic_disease
    WHERE data_value IS NOT NULL
      AND data_value_type = 'Age-adjusted prevalence'
    GROUP BY location_desc, category
)
SELECT
    state,
    category,
    ROUND(avg_prevalence::NUMERIC, 2)               AS avg_prevalence,
    NTILE(4) OVER (
        PARTITION BY category
        ORDER BY avg_prevalence
    )                                               AS quartile,
    ROUND(PERCENT_RANK() OVER (
        PARTITION BY category
        ORDER BY avg_prevalence
    ) * 100, 1)                                     AS percentile_rank
FROM state_avg
ORDER BY category, avg_prevalence DESC;


-- 2b. Year over year change in chronic disease rates
WITH yearly AS (
    SELECT
        year_start                      AS yr,
        category,
        AVG(data_value)                 AS avg_value
    FROM raw_chronic_disease
    WHERE data_value IS NOT NULL
    GROUP BY year_start, category
)
SELECT
    yr,
    category,
    ROUND(avg_value::NUMERIC, 2)        AS avg_value,
    ROUND(LAG(avg_value) OVER (
        PARTITION BY category
        ORDER BY yr
    )::NUMERIC, 2)                      AS prev_year_value,
    ROUND((avg_value - LAG(avg_value) OVER (
        PARTITION BY category ORDER BY yr
    ))::NUMERIC, 2)                     AS yoy_change,
    ROUND(100.0 * (avg_value - LAG(avg_value) OVER (
        PARTITION BY category ORDER BY yr
    )) / NULLIF(LAG(avg_value) OVER (
        PARTITION BY category ORDER BY yr
    ), 0), 1)                           AS yoy_pct_change
FROM yearly
ORDER BY category, yr;


-- 2c. States with highest burden across ALL chronic disease categories
WITH state_scores AS (
    SELECT
        location_desc                   AS state,
        category,
        AVG(data_value)                 AS avg_value,
        RANK() OVER (
            PARTITION BY category
            ORDER BY AVG(data_value) DESC
        )                               AS category_rank
    FROM raw_chronic_disease
    WHERE data_value IS NOT NULL
    GROUP BY location_desc, category
)
SELECT
    state,
    COUNT(*) FILTER (WHERE category_rank <= 10)     AS times_in_top10,
    ROUND(AVG(avg_value)::NUMERIC, 2)               AS overall_avg,
    STRING_AGG(
        category || ' (#' || category_rank || ')',
        ', ' ORDER BY category_rank
    ) FILTER (WHERE category_rank <= 5)             AS top5_categories
FROM state_scores
GROUP BY state
ORDER BY times_in_top10 DESC, overall_avg DESC
LIMIT 20;


-- =============================================================================
-- 3. VACCINATION ANALYSIS
-- =============================================================================

-- 3a. Vaccination progress by state with completion rate
SELECT
    location,
    MAX(series_complete)                            AS fully_vaccinated,
    MAX(administered_dose1)                         AS at_least_one_dose,
    MAX(additional_doses)                           AS booster_doses,
    ROUND(
        100.0 * MAX(series_complete) /
        NULLIF(MAX(administered_dose1), 0), 1
    )                                               AS completion_rate_pct,
    RANK() OVER (
        ORDER BY MAX(series_complete) DESC
    )                                               AS vaccination_rank
FROM raw_vaccination
WHERE location NOT IN ('US', 'PR', 'VI', 'GU', 'MP', 'AS', 'FM', 'MH', 'PW')
  AND series_complete IS NOT NULL
GROUP BY location
ORDER BY fully_vaccinated DESC;


-- 3b. Weekly vaccination rollout speed (doses per week)
WITH weekly_doses AS (
    SELECT
        date,
        location,
        administered_dose1,
        administered_dose1 - LAG(administered_dose1) OVER (
            PARTITION BY location ORDER BY date
        )                                           AS new_doses_this_week
    FROM raw_vaccination
    WHERE administered_dose1 IS NOT NULL
)
SELECT
    date,
    SUM(new_doses_this_week)                        AS national_weekly_doses,
    AVG(SUM(new_doses_this_week)) OVER (
        ORDER BY date
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    )                                               AS rolling_4wk_avg
FROM weekly_doses
WHERE new_doses_this_week > 0
GROUP BY date
ORDER BY date;


-- =============================================================================
-- 4. PIPELINE HEALTH MONITORING
-- =============================================================================

-- 4a. Pipeline audit summary
SELECT
    pipeline_name,
    COUNT(*)                                        AS total_runs,
    SUM(rows_inserted)                              AS total_rows_inserted,
    SUM(rows_failed)                                AS total_rows_failed,
    ROUND(
        100.0 * SUM(rows_inserted) /
        NULLIF(SUM(rows_fetched), 0), 2
    )                                               AS success_rate_pct,
    COUNT(*) FILTER (WHERE status = 'SUCCESS')      AS successful_runs,
    COUNT(*) FILTER (WHERE status = 'FAILED')       AS failed_runs,
    MAX(completed_at)                               AS last_run
FROM pipeline_audit
GROUP BY pipeline_name
ORDER BY last_run DESC;


-- 4b. Data freshness check
SELECT
    'covid_deaths'                                  AS table_name,
    MAX(ingested_at)                                AS last_ingested,
    NOW() - MAX(ingested_at)                        AS data_age,
    COUNT(*)                                        AS total_records
FROM raw_covid_deaths
UNION ALL
SELECT
    'chronic_disease',
    MAX(ingested_at),
    NOW() - MAX(ingested_at),
    COUNT(*)
FROM raw_chronic_disease
UNION ALL
SELECT
    'vaccination',
    MAX(ingested_at),
    NOW() - MAX(ingested_at),
    COUNT(*)
FROM raw_vaccination
ORDER BY last_ingested DESC;
