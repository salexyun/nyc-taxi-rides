{{ config(materialized='view') }}

with tripdata as 
(
  select *,
    row_number() over(partition by dropoff_datetime) as rn
  from {{ source('staging', 'fhv_tripdata_partitioned') }}
)
select
    cast(dispatching_base_num as STRING) as dispatching_base_num,
    cast(pickup_datetime as TIMESTAMP) as pickup_datetime,
    cast(dropoff_datetime as TIMESTAMP) as dropoff_datetime,
    cast(PULocationID as INTEGER) as pickup_locationid,
    cast(DOLocationID as INTEGER) as dropoff_locationid,
    cast(SR_Flag as INTEGER) as sr_flag
from tripdata

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}