{{
    config(
        materialized='view'
    )
}}

with cycledata as 
(
  select *,
    row_number() over(partition by Rental_Id, Bike_id) as rn
  from {{ source( 'staging','cycling_2019') }}
)


SELECT
    -- identifiers
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

from cycledata
where rn = 1


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}