## Module 2 Homework

For the homework, we'll be working with the _green_ taxi dataset located here:

`https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/green/download`

To get a `wget`-able link, use this prefix (note that the link itself gives 404):

`https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/`

### Assignment

So far in the course, we processed data for the year 2019 and 2020. Your task is to extend the existing flows to include data for the year 2021.

As a hint, Kestra makes that process really easy:
1. You can leverage the backfill functionality in the [scheduled flow](../../../02-workflow-orchestration/flows/09_gcp_taxi_scheduled.yaml) to backfill the data for the year 2021. Just make sure to select the time period for which data exists i.e. from `2021-01-01` to `2021-07-31`. Also, make sure to do the same for both `yellow` and `green` taxi data (select the right service in the `taxi` input).
2. Alternatively, run the flow manually for each of the seven months of 2021 for both `yellow` and `green` taxi data. Challenge for you: find out how to loop over the combination of Year-Month and `taxi`-type using `ForEach` task which triggers the flow for each combination using a `Subflow` task.

### Quiz Questions

Complete the quiz shown below. It's a set of 6 multiple-choice questions to test your understanding of workflow orchestration, Kestra, and ETL pipelines.

1) Within the execution for `Yellow` Taxi data for the year `2020` and month `12`: what is the uncompressed file size (i.e. the output file `yellow_tripdata_2020-12.csv` of the `extract` task)?
- 128.3 MiB
- 134.5 MiB
- 364.7 MiB
- 692.6 MiB

**Answer**: File size can be found by looking for the `upload_to_gcs` task row in the in the metrics table, which is listed as 134,481,400 bytes. This can be converted to MiB by multiplying by the conversion factor $\frac{1 \text{MiB}}{1024\times1024 \text{ bytes}}$, which yields 128.3 MiB.

2) What is the rendered value of the variable `file` when the inputs `taxi` is set to `green`, `year` is set to `2020`, and `month` is set to `04` during execution?
- `{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv` 
- `green_tripdata_2020-04.csv`
- `green_tripdata_04_2020.csv`
- `green_tripdata_2020.csv`

**Answer**: `green_tripdata_2020-04.csv`. This is obvious from the definition of the variable `file` in 08_gcp_taxi.yaml:
```yaml
variables:
  file: "{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv"
```

*For the following questions, we will run the 09_gcp_taxi_scheduled.yaml flow for the all the months in the specified year for either yellow or green cabs by creating a Backfill and entering the first day of the specified year as the start date and the last day of the next year as the end date, and executing for either yellow or green cabs.*

3) How many rows are there for the `Yellow` Taxi data for all CSV files in the year 2020?
- 13,537,299
- 24,648,499
- 18,324,219
- 29,430,127

**Answer**: To count the number of rows for all files we can query the BigQuery database:
```sql
SELECT COUNT(*) AS total_rows
  FROM (
    SELECT * FROM `zoomcamp.yellow_tripdata_2020_01_ext`
    UNION ALL
    SELECT * FROM `zoomcamp.yellow_tripdata_2020_02_ext`
    UNION ALL
    SELECT * FROM `zoomcamp.yellow_tripdata_2020_03_ext`
    UNION ALL
    SELECT * FROM `zoomcamp.yellow_tripdata_2020_04_ext`
    UNION ALL
    SELECT * FROM `zoomcamp.yellow_tripdata_2020_05_ext`
    UNION ALL
    SELECT * FROM `zoomcamp.yellow_tripdata_2020_06_ext`
    UNION ALL
    SELECT * FROM `zoomcamp.yellow_tripdata_2020_07_ext`
    UNION ALL
    SELECT * FROM `zoomcamp.yellow_tripdata_2020_08_ext`
    UNION ALL
    SELECT * FROM `zoomcamp.yellow_tripdata_2020_09_ext`
    UNION ALL
    SELECT * FROM `zoomcamp.yellow_tripdata_2020_10_ext`
    UNION ALL
    SELECT * FROM `zoomcamp.yellow_tripdata_2020_11_ext`
    UNION ALL
    SELECT * FROM `zoomcamp.yellow_tripdata_2020_12_ext`
);
```
| total_rows |
| :---: |
| 24648499 |

4) How many rows are there for the `Green` Taxi data for all CSV files in the year 2020?
- 5,327,301
- 936,199
- 1,734,051
- 1,342,034

**Answer:**
```sql
SELECT COUNT(*) AS total_rows
  FROM (
    SELECT * FROM `zoomcamp.green_tripdata_2020_01_ext`
    UNION ALL
    SELECT * FROM `zoomcamp.green_tripdata_2020_02_ext`
    UNION ALL
    SELECT * FROM `zoomcamp.green_tripdata_2020_03_ext`
    UNION ALL
    SELECT * FROM `zoomcamp.green_tripdata_2020_04_ext`
    UNION ALL
    SELECT * FROM `zoomcamp.green_tripdata_2020_05_ext`
    UNION ALL
    SELECT * FROM `zoomcamp.green_tripdata_2020_06_ext`
    UNION ALL
    SELECT * FROM `zoomcamp.green_tripdata_2020_07_ext`
    UNION ALL
    SELECT * FROM `zoomcamp.green_tripdata_2020_08_ext`
    UNION ALL
    SELECT * FROM `zoomcamp.green_tripdata_2020_09_ext`
    UNION ALL
    SELECT * FROM `zoomcamp.green_tripdata_2020_10_ext`
    UNION ALL
    SELECT * FROM `zoomcamp.green_tripdata_2020_11_ext`
    UNION ALL
    SELECT * FROM `zoomcamp.green_tripdata_2020_12_ext`
);
```
| total_rows |
| :---: |
| 1734051 |

*Here we can run 08_gcp_taxi.yaml for yellow cabs in March 2021.*

5) How many rows are there for the `Yellow` Taxi data for the March 2021 CSV file?
- 1,428,092
- 706,911
- 1,925,152
- 2,561,031

**Answer:**
```sql
SELECT COUNT(*) AS total_rows
  FROM `zoomcamp.yellow_tripdata_2021_03_ext`;
```
| total_rows |
| :---: |
| 1925152 |

6) How would you configure the timezone to New York in a Schedule trigger?
- Add a `timezone` property set to `EST` in the `Schedule` trigger configuration  
- Add a `timezone` property set to `America/New_York` in the `Schedule` trigger configuration
- Add a `timezone` property set to `UTC-5` in the `Schedule` trigger configuration
- Add a `location` property set to `New_York` in the `Schedule` trigger configuration  

**Answer:** Add a `timezone` property set to `America/New_York` in the `Schedule` trigger configuration (according to the Kestra documentation).

```yaml
triggers:
  - id: green_schedule
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 9 1 * *"
    timezone: America/New_York
    inputs:
      taxi: green
```