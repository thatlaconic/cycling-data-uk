{{
    config(
        materialized = 'table'
    )
}}


SELECT 
    CASE 
        WHEN EXTRACT(DAYOFWEEK FROM Start_Date) IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    COUNT(*) / COUNT(DISTINCT DATE(Start_Date)) AS avg_rentals_per_day
FROM {{ ref('cycles_2019-2024') }}
WHERE EXTRACT(YEAR FROM Start_Date) >= 2023
GROUP BY day_type