# Module 3 Homework: Data Warehousing & BigQuery

In this homework we'll practice working with BigQuery and Google Cloud Storage.

When submitting your homework, you will also need to include
a link to your GitHub repository or other public code-hosting
site.

This repository should contain the code for solving the homework.

When your solution has SQL or shell commands and not code
(e.g. python files) file format, include them directly in
the README file of your repository.

## Data

For this homework we will be using the Yellow Taxi Trip Records for January 2024 - June 2024 (not the entire year of data).

Parquet Files are available from the New York City Taxi Data found here:

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Loading the data

You can use the following scripts to load the data into your GCS bucket:

- Python script: [load_yellow_taxi_data.py](./load_yellow_taxi_data.py)
- Jupyter notebook with DLT: [DLT_upload_to_GCP.ipynb](./DLT_upload_to_GCP.ipynb)

You will need to generate a Service Account with GCS Admin privileges or be authenticated with the Google SDK, and update the bucket name in the script.

If you are using orchestration tools such as Kestra, Mage, Airflow, or Prefect, do not load the data into BigQuery using the orchestrator.

Make sure that all 6 files show in your GCS bucket before beginning.

Note: You will need to use the PARQUET option when creating an external table.


## BigQuery Setup

Create an external table using the Yellow Taxi Trip Records. 

Create a (regular/materialized) table in BQ using the Yellow Taxi Trip Records (do not partition or cluster this table). 



## Question 1. Counting records

What is count of records for the 2024 Yellow Taxi Data?
- 65,623
- 840,402
- 20,332,093
- 85,431,289

```sql
SELECT COUNT(*)
FROM dtc-de-course-491308.nyc_yellow_taxi.yellow_tripdata_non_partitioned;
```
**Answer**: 20,332,093

## Question 2. Data read estimation

Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
 
What is the **estimated amount** of data that will be read when this query is executed on the External Table and the Table?

- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- 0 MB for the External Table and 155.12 MB for the Materialized Table
- 2.14 GB for the External Table and 0MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table

```sql
SELECT COUNT(DISTINCT PULocationID)
FROM dtc-de-course-491308.nyc_yellow_taxi.external_yellow_tripdata;
```
```
This query will process 0 B when run.
```

```sql
SELECT COUNT(DISTINCT PULocationID)
FROM dtc-de-course-491308.nyc_yellow_taxi.yellow_tripdata_non_partitioned;
```
```
This query will process 155.12 MB when run.
```
**Answer**: 0 MB for the External Table and 155.12 MB for the Materialized Table. Data stays in GCS in external tables, and BigQuery just reads it on demand. On the other hand, Materialized Tables, as the name suggests, do have their data loaded into BigQuery storage and stored in its optimized columnar format. This allows BigQuery to accurately estimate (before execution) the amount of processed bytes, unlike in the external table, where it cannot estimate it because the table lives in GCS.

## Question 3. Understanding columnar storage

Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table.

Why are the estimated number of Bytes different?
- BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires 
reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.
- BigQuery duplicates data across multiple storage partitions, so selecting two columns instead of one requires scanning the table twice, 
doubling the estimated bytes processed.
- BigQuery automatically caches the first queried column, so adding a second column increases processing time but does not affect the estimated bytes scanned.
- When selecting multiple columns, BigQuery performs an implicit join operation between them, increasing the estimated bytes processed

```sql
SELECT PULocationID
FROM dtc-de-course-491308.nyc_yellow_taxi.yellow_tripdata_non_partitioned;
```
```
This query will process 155.12 MB when run.
```

```sql
SELECT PULocationID,
       DOLocationID
FROM dtc-de-course-491308.nyc_yellow_taxi.yellow_tripdata_non_partitioned;
```
```
This query will process 310.24 MB when run.
```
$$310.24 \text{ MB} = 155.12 \times 2 \text{ MB}$$

**Answer**: BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires 
reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

## Question 4. Counting zero fare trips

How many records have a fare_amount of 0?
- 128,210
- 546,578
- 20,188,016
- 8,333

```sql
SELECT COUNT(*)
FROM dtc-de-course-491308.nyc_yellow_taxi.yellow_tripdata_non_partitioned
WHERE fare_amount = 0;
```

**Answer**: 8,333

## Question 5. Partitioning and clustering

What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)

- Partition by tpep_dropoff_datetime and Cluster on VendorID
- Cluster on by tpep_dropoff_datetime and Cluster on VendorID
- Cluster on tpep_dropoff_datetime Partition by VendorID
- Partition by tpep_dropoff_datetime and Partition by VendorID

```sql
CREATE OR REPLACE TABLE dtc-de-course-491308.nyc_yellow_taxi.external_yellow_tripdata_partitioned_clustered
PARTITION BY DATE(tpep_dropoff_datetime)
  CLUSTER BY VendorID
AS SELECT * FROM dtc-de-course-491308.nyc_yellow_taxi.yellow_tripdata_non_partitioned;
```

**Answer:** Partition by tpep_dropoff_datetime and Cluster on VendorID. The former has much lower cardinality.

## Question 6. Partition benefits

Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime
2024-03-01 and 2024-03-15 (inclusive)


Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values? 


Choose the answer which most closely matches.
 

- 12.47 MB for non-partitioned table and 326.42 MB for the partitioned table
- 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table
- 5.87 MB for non-partitioned table and 0 MB for the partitioned table
- 310.31 MB for non-partitioned table and 285.64 MB for the partitioned table

```sql
SELECT DISTINCT VendorID
FROM dtc-de-course-491308.nyc_yellow_taxi.yellow_tripdata_non_partitioned
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
```
```
This query will process 310.24 MB when run.
```

```sql
SELECT DISTINCT VendorID
FROM dtc-de-course-491308.nyc_yellow_taxi.yellow_tripdata_partitioned_clustered
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
```
```
This query will process 26.84 MB when run.
```

**Answer**: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table. The estimated bytes processed in the partitioned and clustered table as much fewer because BigQuery only scans the corresponding partitions to the date range specified, i.e., for the first 15 days of March, skipping over rest of March and the other months that need to be scanned in the non-partitioned table.

## Question 7. External table storage

Where is the data stored in the External Table you created?

- Big Query
- Container Registry
- GCP Bucket
- Big Table

**Answer**: GCP Bucket. BigQuery only references the data in the external table.

## Question 8. Clustering best practices

It is best practice in Big Query to always cluster your data:
- True
- False

**Answer**: False. Clustering is best practice for very large datasets. Unlike partitioning, it does not have a limit in the number of clusters, thereby optimizing high-cardinality columns. However, clustering does not provide benefits when the table is small, the targeted columns have very low cardinality, or when there isn't a consistent query pattern.

## Question 9. Understanding table scans

No Points: Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

```sql
SELECT COUNT(*)
FROM dtc-de-course-491308.nyc_yellow_taxi.yellow_tripdata_non_partitioned
WHERE fare_amount = 0;
```

0 bytes. Because the BigQuery does not need to read the data iself, it never touches the columns in storage. It just retrieves the row count from the table's metadata.


## Submitting the solutions

Form for submitting: https://courses.datatalks.club/de-zoomcamp-2026/homework/hw3


## Learning in Public

We encourage everyone to share what they learned. This is called "learning in public".

Read more about the benefits [here](https://alexeyondata.substack.com/p/benefits-of-learning-in-public-and).

### Example post for LinkedIn

```
🚀 Week 3 of Data Engineering Zoomcamp by @DataTalksClub complete!

Just finished Module 3 - Data Warehousing with BigQuery. Learned how to:

✅ Create external tables from GCS bucket data
✅ Build materialized tables in BigQuery
✅ Partition and cluster tables for performance
✅ Understand columnar storage and query optimization
✅ Analyze NYC taxi data at scale

Working with 20M+ records and learning how partitioning reduces query costs!

Here's my homework solution: <LINK>

Following along with this amazing free course - who else is learning data engineering?

You can sign up here: https://github.com/DataTalksClub/data-engineering-zoomcamp/
```

### Example post for Twitter/X

```
📊 Module 3 of Data Engineering Zoomcamp done!

- BigQuery & GCS
- External vs materialized tables
- Partitioning & clustering
- Query optimization

My solution: <LINK>

Free course by @DataTalksClub: https://github.com/DataTalksClub/data-engineering-zoomcamp/
```

