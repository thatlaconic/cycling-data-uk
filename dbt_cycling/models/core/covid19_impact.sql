{{
    config(
        materialized = 'table'
    )
}}


SELECT 
    EXTRACT(YEAR FROM Start_Date) AS year,
    EXTRACT(QUARTER FROM Start_Date) AS quarter,
    COUNT(*) AS rentals
FROM {{ ref('cycles_2019-2024') }}
WHERE EXTRACT(YEAR FROM Start_Date) BETWEEN 2019 AND 2021
GROUP BY year, quarter
ORDER BY year, quarter