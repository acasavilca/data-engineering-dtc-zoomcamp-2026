DATASET_DTYPES = {
    "green_tripdata_2025-11": {
        "DTYPES": {
            "VendorID": "Int64",
            "store_and_fwd_flag": "string",
            "RatecodeID": "Int64",
            "PULocationID": "Int64",
            "DOLocationID": "Int64",
            "passenger_count": "Int64",
            "trip_distance": "float64",
            "fare_amount": "float64",
            "extra": "float64",
            "mta_tax": "float64",
            "tip_amount": "float64",
            "tolls_amount": "float64",
            "ehail_fee": "float64",
            "improvement_surcharge": "float64",
            "total_amount": "float64",
            "payment_type": "Int64",
            "trip_type": "Int64",
            "congestion_surcharge": "float64",
            "cbd_congestion_fee": "float64",    
        },
        "DATETIME_COLS": [
            "lpep_pickup_datetime",
            "lpep_dropoff_datetime", 
        ],
    },
    "taxi_zone_lookup": {
        "DTYPES": {
            "LocationID": "Int64",
            "Borough": "string",
            "Zone": "string",
            "service_zone": "string",        
        },
        "DATETIME_COLS": [],
    },
}
