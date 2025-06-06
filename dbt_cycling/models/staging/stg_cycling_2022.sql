{{
    config(
        materialized='view'
    )
}}


with date1 as (SELECT
-- identifiers
    {{ dbt_utils.generate_surrogate_key(['Rental_Id', 'Bike_id']) }} as tripid,
    {{ dbt.safe_cast("Rental_Id", api.Column.translate_type("integer")) }} as Rental_Id,
    {{ dbt.safe_cast("Duration", api.Column.translate_type("integer")) }} as Duration,
    {{ dbt.safe_cast("Bike_Id", api.Column.translate_type("integer")) }} as Bike_Id,
    DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M', End_Date)) AS End_Date,
    TIME(PARSE_TIMESTAMP('%Y-%m-%d %H:%M', End_Date)) AS End_Date_Time,
    {{ dbt.safe_cast("EndStation_Id", api.Column.translate_type("integer")) }} as EndStation_Id,
    cast(EndStation_Name as string) as EndStation_Name,
    DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M', Start_Date)) AS Start_Date,
    TIME(PARSE_TIMESTAMP('%Y-%m-%d %H:%M', Start_Date)) AS Start_Date_Time,
    {{ dbt.safe_cast("StartStation_Id", api.Column.translate_type("integer")) }} as StartStation_Id,
    cast(StartStation_Name as string) as StartStation_Name
FROM {{ source( 'staging','cycling_2022') }}
WHERE End_Date LIKE '%-%' AND
Rental_Id IS NOT NULL AND
Bike_Id IS NOT NULL
),
date2 as ( 
  SELECT 
    {{ dbt_utils.generate_surrogate_key(['Rental_Id', 'Bike_id']) }} as tripid,
    {{ dbt.safe_cast("Rental_Id", api.Column.translate_type("integer")) }} as Rental_Id,
    {{ dbt.safe_cast("Duration", api.Column.translate_type("integer")) }} as Duration,
    {{ dbt.safe_cast("Bike_Id", api.Column.translate_type("integer")) }} as Bike_Id,
    DATE(FORMAT_DATE('%Y-%m-%d', DATE(PARSE_TIMESTAMP('%d/%m/%Y %H:%M', End_Date)))) AS End_Date, 
    TIME(PARSE_TIMESTAMP('%d/%m/%Y %H:%M', End_Date)) AS End_Date_Time,
    {{ dbt.safe_cast("EndStation_Id", api.Column.translate_type("integer")) }} as EndStation_Id,
    cast(EndStation_Name as string) as EndStation_Name,
    DATE(FORMAT_DATE('%Y-%m-%d',DATE(PARSE_TIMESTAMP('%d/%m/%Y %H:%M', Start_Date)))) AS Start_Date,
    TIME(PARSE_TIMESTAMP('%d/%m/%Y %H:%M', Start_Date)) AS Start_Date_Time,
    {{ dbt.safe_cast("StartStation_Id", api.Column.translate_type("integer")) }} as StartStation_Id,
    cast(StartStation_Name as string) as StartStation_Name
  FROM {{ source( 'staging','cycling_2022') }}
  WHERE End_Date LIKE '%/%' AND
  Rental_Id IS NOT NULL AND
  Bike_Id IS NOT NULL
    ),

union_all as (
  SELECT * FROM date1
  union all
  SELECT * FROM date2
),

cycledata as 
(
  select *,
    row_number() over(partition by Rental_Id, Bike_id) as rn
  FROM union_all
)

SELECT tripid, Rental_Id, Duration, Bike_Id, End_Date, End_Date_Time, EndStation_Id,
EndStation_Name, Start_Date, Start_Date_Time, StartStation_Id, StartStation_Name
FROM cycledata
WHERE rn = 1


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}