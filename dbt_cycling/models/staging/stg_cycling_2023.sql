{{
    config(
        materialized='view'
    )
}}

with cycledata as 
(
  select *, ROUND(CAST(Total_duration__ms_ AS NUMERIC)/1000, 0) AS Duration,
    row_number() over(partition by Number, Bike_number) as rn
  from {{ source( 'staging','cycling_2023') }}
)


SELECT
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['Number', 'Bike_number']) }} as tripid,
    {{ dbt.safe_cast("Number", api.Column.translate_type("integer")) }} as Rental_Id,
    {{ dbt.safe_cast("Duration", api.Column.translate_type("integer")) }} as Duration,
    {{ dbt.safe_cast("Bike_number", api.Column.translate_type("integer")) }} as Bike_Id,
    DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M', End_date)) AS End_Date,
    TIME(PARSE_TIMESTAMP('%Y-%m-%d %H:%M', End_date)) AS End_Date_Time,
    {{ dbt.safe_cast("End_station_number", api.Column.translate_type("integer")) }} as EndStation_Id,
    cast(End_station as string) as EndStation_Name,
    DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M', Start_date)) AS Start_Date,
    TIME(PARSE_TIMESTAMP('%Y-%m-%d %H:%M', Start_date)) AS Start_Date_Time,
    {{ dbt.safe_cast("Start_station_number", api.Column.translate_type("integer")) }} as StartStation_Id,
    cast(Start_station as string) as StartStation_Name

from cycledata
where rn = 1


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}