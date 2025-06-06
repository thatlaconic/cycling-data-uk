{{
    config(
        materialized = 'table'
    )
}}


WITH station_2023 AS (
    SELECT 
        StartStation_Name,
        COUNT(*) AS rentals_2023,
        RANK() OVER (ORDER BY COUNT(*) DESC) AS rank_2023
    FROM {{ ref('cycles_2019-2024') }}
    WHERE EXTRACT(YEAR FROM Start_Date) = 2023
    GROUP BY StartStation_Name
),
station_2024 AS (
    SELECT 
        StartStation_Name,
        COUNT(*) AS rentals_2024,
        RANK() OVER (ORDER BY COUNT(*) DESC) AS rank_2024
    FROM {{ ref('cycles_2019-2024') }}
    WHERE EXTRACT(YEAR FROM Start_Date) = 2024
    GROUP BY StartStation_Name
)
SELECT 
    s23.StartStation_Name,
    s23.rentals_2023,
    s24.rentals_2024,
    (s24.rentals_2024 - s23.rentals_2023) AS YoY_change
FROM station_2023 s23
JOIN station_2024 s24 ON s23.StartStation_Name = s24.StartStation_Name
WHERE s23.rank_2023 <= 5 OR s24.rank_2024 <= 5
ORDER BY YoY_change DESC