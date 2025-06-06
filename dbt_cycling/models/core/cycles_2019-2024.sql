{{
    config(
        materialized='table',
        partition_by = {
            "field": "End_Date",
            "data_type": "date",
            "granularity": "year"
    }
    )
}}

with cycle2019 as (
    select *, 
    from {{ ref('stg_cycling_2019') }}
), 
cycle2020 as (
    select *, 
    from {{ ref('stg_cycling_2020') }}
), 
cycle2021 as (
    select *, 
    from {{ ref('stg_cycling_2021') }}
),
cycle2022 as (
    select *, 
    from {{ ref('stg_cycling_2022') }}
),
cycle2023 as (
    select *, 
    from {{ ref('stg_cycling_2023') }}
),
cycle2024 as (
    select *, 
    from {{ ref('stg_cycling_2024') }}
)

select * from cycle2019
union all 
select * from cycle2020
union all
select * from cycle2021
union all
select * from cycle2022
union all
select * from cycle2023
union all
select * from cycle2024
