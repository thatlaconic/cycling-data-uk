{{
    config(
        materialized = 'table'
    )
}}

SELECT 
    EXTRACT(YEAR FROM Start_Date) AS year,
    AVG(Duration / 60) AS avg_duration_minutes
FROM {{ ref('cycles_2019-2024') }}
GROUP BY year
ORDER BY year