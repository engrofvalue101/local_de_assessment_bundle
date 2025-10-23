-- models/gold/dimensions/dim_date.sql
{{
    config(
        materialized='table',
        schema='gold'
    )
}}

WITH date_spine AS (
    -- Generate 15 years of dates from 2015-01-01
    SELECT 
        DATE '2015-01-01' + (n * INTERVAL 1 DAY) AS date_day
    FROM range(0, 5475) AS t(n)
),

date_attributes AS (
    SELECT
        date_day,
        
        -- Basic date components
        EXTRACT(YEAR FROM date_day) AS year,
        EXTRACT(MONTH FROM date_day) AS month,
        EXTRACT(DAY FROM date_day) AS day,
        EXTRACT(QUARTER FROM date_day) AS quarter,
        EXTRACT(DOW FROM date_day) AS day_of_week,  -- Sunday=0, Saturday=6
        EXTRACT(DOY FROM date_day) AS day_of_year,
        EXTRACT(WEEK FROM date_day) AS iso_week,
        
        -- Formatted strings
        strftime(date_day, '%Y-%m') AS year_month,
        strftime(date_day, '%Y-') || cast(extract(quarter from date_day) as varchar) AS year_quarter,
        strftime(date_day, '%B') AS month_name,
        strftime(date_day, '%b') AS month_short,
        strftime(date_day, '%A') AS day_name,
        strftime(date_day, '%a') AS day_short,
        
        -- Week indicators
        CASE WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
        CASE WHEN EXTRACT(DOW FROM date_day) BETWEEN 1 AND 5 THEN TRUE ELSE FALSE END AS is_weekday,
        
        -- Australian Fiscal Year (July 1 - June 30)
        CASE 
            WHEN EXTRACT(MONTH FROM date_day) >= 7 
            THEN EXTRACT(YEAR FROM date_day) + 1
            ELSE EXTRACT(YEAR FROM date_day)
        END AS fiscal_year,
        
        CASE 
            WHEN EXTRACT(MONTH FROM date_day) BETWEEN 7 AND 9 THEN 1
            WHEN EXTRACT(MONTH FROM date_day) BETWEEN 10 AND 12 THEN 2
            WHEN EXTRACT(MONTH FROM date_day) BETWEEN 1 AND 3 THEN 3
            WHEN EXTRACT(MONTH FROM date_day) BETWEEN 4 AND 6 THEN 4
        END AS fiscal_quarter,
        
        -- First/last day indicators
        CASE WHEN EXTRACT(DAY FROM date_day) = 1 THEN TRUE ELSE FALSE END AS is_first_day_of_month,
        CASE 
            WHEN date_day = (date_trunc('month', date_day) + INTERVAL 1 month - INTERVAL 1 day)
            THEN TRUE 
            ELSE FALSE 
        END AS is_last_day_of_month,
        
        -- Quarter boundaries
        CASE 
            WHEN EXTRACT(MONTH FROM date_day) IN (1, 4, 7, 10) AND EXTRACT(DAY FROM date_day) = 1 
            THEN TRUE 
            ELSE FALSE 
        END AS is_first_day_of_quarter,
        
        CASE 
            WHEN EXTRACT(MONTH FROM date_day) IN (3, 6, 9, 12) 
                AND date_day = (date_trunc('month', date_day) + INTERVAL 1 month - INTERVAL 1 day)
            THEN TRUE 
            ELSE FALSE 
        END AS is_last_day_of_quarter
        
    FROM date_spine
),

holidays AS (
    SELECT
        date_day,
        
        CASE
            WHEN EXTRACT(MONTH FROM date_day) = 1 AND EXTRACT(DAY FROM date_day) = 1 THEN 'New Year''s Day'
            WHEN EXTRACT(MONTH FROM date_day) = 1 AND EXTRACT(DAY FROM date_day) = 26 THEN 'Australia Day'
            WHEN EXTRACT(MONTH FROM date_day) = 4 AND EXTRACT(DAY FROM date_day) = 25 THEN 'ANZAC Day'
            WHEN EXTRACT(MONTH FROM date_day) = 12 AND EXTRACT(DAY FROM date_day) = 25 THEN 'Christmas Day'
            WHEN EXTRACT(MONTH FROM date_day) = 12 AND EXTRACT(DAY FROM date_day) = 26 THEN 'Boxing Day'
            WHEN EXTRACT(MONTH FROM date_day) = 4 AND EXTRACT(DOW FROM date_day) = 1 AND EXTRACT(DAY FROM date_day) BETWEEN 1 AND 30 
                THEN 'Easter Monday (approx)'
            WHEN EXTRACT(MONTH FROM date_day) = 6 AND EXTRACT(DOW FROM date_day) = 1 AND EXTRACT(DAY FROM date_day) BETWEEN 8 AND 14 
                THEN 'Queen''s Birthday'
            ELSE NULL
        END AS holiday_name,
        
        CASE
            WHEN EXTRACT(MONTH FROM date_day) = 1 AND EXTRACT(DAY FROM date_day) IN (1, 26) THEN TRUE
            WHEN EXTRACT(MONTH FROM date_day) = 4 AND EXTRACT(DAY FROM date_day) = 25 THEN TRUE
            WHEN EXTRACT(MONTH FROM date_day) = 12 AND EXTRACT(DAY FROM date_day) IN (25, 26) THEN TRUE
            WHEN EXTRACT(MONTH FROM date_day) = 6 AND EXTRACT(DOW FROM date_day) = 1 AND EXTRACT(DAY FROM date_day) BETWEEN 8 AND 14 THEN TRUE
            ELSE FALSE
        END AS is_holiday
        
    FROM date_attributes
),

final_date_dimension AS (
    SELECT
        da.date_day,
        da.year,
        da.month,
        da.day,
        da.quarter,
        da.day_of_week,
        da.day_of_year,
        da.iso_week,
        da.year_month,
        da.year_quarter,
        da.month_name,
        da.month_short,
        da.day_name,
        da.day_short,
        da.is_weekend,
        da.is_weekday,
        da.fiscal_year,
        da.fiscal_quarter,
        da.is_first_day_of_month,
        da.is_last_day_of_month,
        da.is_first_day_of_quarter,
        da.is_last_day_of_quarter,
        h.holiday_name,
        h.is_holiday,
        
        CASE 
            WHEN da.is_weekday = TRUE AND h.is_holiday = FALSE THEN TRUE 
            ELSE FALSE 
        END AS is_business_day,
        
        'FY' || cast(da.fiscal_year as varchar) AS fiscal_year_label,
        'FY' || cast(da.fiscal_year as varchar) || '-Q' || cast(da.fiscal_quarter as varchar) AS fiscal_quarter_label,
        
        CASE WHEN da.date_day = current_date THEN TRUE ELSE FALSE END AS is_today,
        CASE WHEN da.date_day = current_date - INTERVAL 1 day THEN TRUE ELSE FALSE END AS is_yesterday,
        CASE WHEN da.date_day BETWEEN current_date - INTERVAL 7 day AND current_date THEN TRUE ELSE FALSE END AS is_last_7_days,
        CASE WHEN da.date_day BETWEEN current_date - INTERVAL 30 day AND current_date THEN TRUE ELSE FALSE END AS is_last_30_days,
        CASE WHEN EXTRACT(MONTH FROM da.date_day) = EXTRACT(MONTH FROM current_date) 
             AND EXTRACT(YEAR FROM da.date_day) = EXTRACT(YEAR FROM current_date) THEN TRUE ELSE FALSE END AS is_current_month,
        CASE WHEN EXTRACT(QUARTER FROM da.date_day) = EXTRACT(QUARTER FROM current_date) 
             AND EXTRACT(YEAR FROM da.date_day) = EXTRACT(YEAR FROM current_date) THEN TRUE ELSE FALSE END AS is_current_quarter,
        CASE WHEN EXTRACT(YEAR FROM da.date_day) = EXTRACT(YEAR FROM current_date) THEN TRUE ELSE FALSE END AS is_current_year
        
    FROM date_attributes da
    JOIN holidays h ON da.date_day = h.date_day
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['date_day']) }} AS date_key,
    *
FROM final_date_dimension
ORDER BY date_day
