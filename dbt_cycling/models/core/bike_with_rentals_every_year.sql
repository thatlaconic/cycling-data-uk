{{
    config(
        materialized = 'table'
    )
}}

SELECT Bike_Id
FROM (
    SELECT Bike_Id, EXTRACT(YEAR FROM Start_Date) AS year
    FROM {{ ref('cycles_2019-2024') }}
    WHERE EXTRACT(YEAR FROM Start_Date) BETWEEN 2019 AND 2024
    GROUP BY Bike_Id, year
) AS yearly_usage
GROUP BY Bike_Id
HAVING COUNT(DISTINCT year) = 6  -- Active all 6 years