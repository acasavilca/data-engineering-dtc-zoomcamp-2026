select 
    -- identifiers
    cast(VendorID as int) as vendor_id,
    cast(RatecodeID as int) as ratecode_id,
    cast(PULocationID as int) as pu_location_id,
    cast(DOLocationID as int) as do_location_id,

    -- timestamps
    cast(lpep_pickup_datetime as timestamp) as lpep_pickup_datetime,
    cast(lpep_dropoff_datetime as timestamp) as lpep_dropoff_datetime,

    -- trip info
    cast(store_and_fwd_flag as bool) as store_and_fwd_flag,
    cast(passenger_count as int) as passenger_count,
    cast(trip_distance as float64) as trip_distance,
    cast(trip_type as int) as trip_type,

    -- payment info
    cast(fare_amount as float64) as fare_amount,
    cast(extra as float64) as extra,
    cast(tip_amount as float64) as tip_amount,
    cast(tolls_amount as float64) as tolls_amount,
    cast(ehail_fee as float64) as ehail_fee,
    cast(improvement_surcharge as float64) as improvement_surcharge,
    cast(total_amount as float64) as total_amount,
    cast(payment_type as int) as payment_type,
    cast(congestion_surcharge as float64) as congestion_surcharge,
    cast(mta_tax as float64) as mta_tax

from {{ source('raw_data', 'green_tripdata_partitioned') }}
where VendorID is not NULL
