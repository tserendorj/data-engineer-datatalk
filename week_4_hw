## Preparation for dataset

{{
    config(
        materialized='view'
    )
}}

select

    dispatching_base_num,    
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
    cast(PULocationID as integer ) as pickup_locationid,
    cast(DOLocationID as integer )  as dropoff_locationid,

    
    sr_flag
    
from {{ source('staging','fhv_tripdata') }}


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}

=========

{{
    config(
        materialized='table'
    )
}}

with fhv_data as (
    select *,
        'Fhv' as service_type
    from {{ ref('stg_fhv_tripdata') }}
),

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    fhv_data.dispatching_base_num,
    fhv_data.service_type,
    fhv_data.pickup_locationid,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    fhv_data.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    fhv_data.pickup_datetime,
    fhv_data.dropoff_datetime,
    fhv_data.sr_flag
from fhv_data
inner join dim_zones as pickup_zone
on fhv_data.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_data.dropoff_locationid = dropoff_zone.locationid




## Question 5:

{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fact_trips') }}
)
    select 
    -- Revenue grouping 
    pickup_zone as revenue_zone,
    {{ dbt.date_trunc("quarter", "pickup_datetime") }} as revenue_quarter, 

    service_type, 

    -- Revenue calculation 
    sum(fare_amount) as revenue_quarterly_fare,
    sum(extra) as revenue_quarterly_extra,
    sum(mta_tax) as revenue_quarterly_mta_tax,
    sum(tip_amount) as revenue_quarterly_tip_amount,
    sum(tolls_amount) as revenue_quarterly_tolls_amount,
    sum(ehail_fee) as revenue_quarterly_ehail_fee,
    sum(improvement_surcharge) as revenue_quarterly_improvement_surcharge,
    sum(total_amount) as revenue_quarterly_total_amount,

    -- Additional calculations
    count(tripid) as total_quarterly_trips,
    avg(passenger_count) as avg_quarterly_passenger_count,
    avg(trip_distance) as avg_quarterly_trip_distance

    from trips_data
    group by 1,2,3

=======

WITH revenue_data AS (
    SELECT 
        DATE_TRUNC(revenue_quarter, QUARTER) AS quarter,
        SUM(revenue_quarterly_total_amount) AS total_revenue
    FROM `terraform-demo-448901.dbt_tserendorj.fct_taxi_trips_quarterly_revenue`
    where service_type = 'Yellow' -- 'Green'
    GROUP BY quarter
),
revenue_with_lag AS (
    SELECT 
        quarter,
        total_revenue,
        LAG(total_revenue, 4) OVER (ORDER BY quarter) AS revenue_last_year
    FROM revenue_data
)
SELECT 
    quarter,
    
    total_revenue,
    revenue_last_year,
    ROUND(SAFE_DIVIDE(total_revenue - revenue_last_year, revenue_last_year) * 100, 2) AS yoy_growth
FROM revenue_with_lag
WHERE revenue_last_year IS NOT NULL and EXTRACT(YEAR FROM TIMESTAMP(quarter)) = 2020
ORDER BY quarter;
