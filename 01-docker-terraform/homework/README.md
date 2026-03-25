# Module 1 Homework: Docker & SQL

In this homework we'll prepare the environment and practice
Docker and SQL

## Question 1. Understanding Docker images
 
Run docker with the `python:3.13` image. Use an entrypoint `bash` to interact with the container.

What's the version of `pip` in the image?

- 25.3
- 24.3.1
- 24.2.1
- 23.3.1

### Solution:
```bash
docker run -it \
--rm \
--entrypoint=bash \
python:3.13
```

```bash
root@efdd95d87422:/# pip -V
pip 25.3 from /usr/local/lib/python3.13/site-packages/pip (python 3.13)
```

## Question 2. Understanding Docker networking and docker-compose

Given the following `docker-compose.yaml`, what is the `hostname` and `port` that pgadmin should use to connect to the postgres database?

```yaml
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```

- postgres:5433
- localhost:5432
- db:5433
- postgres:5432
- db:5432

If multiple answers are correct, select any 

### Solution:

postgres:5432 & db:5432

## Dockerized data ingestion

Use Docker Compose to start and initialize the PostgreSQL database service (wait for a couple of min):
```bash
docker compose up -d
```

Build the Docker image:
```bash
docker build -t taxi_ingest:v001 .
```

Run ingestion for first table:
```bash
docker run -it --rm \
--network=homework_default \
taxi_ingest:v001 \
--pg-user=root \
--pg-pass=root \
--pg-host=pgdatabase \
--pg-port=5432 \
--pg-db=ny_taxi \
--target-table=green_tripdata_2025_11 \
--url="https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet"
```

Run ingestion for second table:
```bash
docker run -it --rm \
--network=homework_default \
taxi_ingest:v001 \
--pg-user=root \
--pg-pass=root \
--pg-host=pgdatabase \
--pg-port=5432 \
--pg-db=ny_taxi \
--target-table=taxi_zone_lookup \
--url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
```

Access databse through pgcli:
```bash
uv run pgcli -h localhost -p 5432 -u root -d ny_taxi
```
```bash
Password for root: root
```

Or pgadmin:
1. **Open pgAdmin** at http://localhost:8080
   - Email: `admin@admin.com`
   - Password: `root`

2. **Register PostgreSQL Server:**
   - Right-click "Servers" → "Register" → "Server..."
   
3. **General tab:**
   - Name: `pg` (or any name)

4. **Connection tab:**
   - Host: `pgdatabase`
   - Port: `5432`
   - Maintenance database: `ny_taxi`
   - Username: `root`
   - Password: `root`

5. **Click "Save"**

You should now see both tables under:
**Servers → pg → Databases → ny_taxi → Schemas → public → Tables**

## Question 3. Counting short trips

For the trips in November 2025 (lpep_pickup_datetime between '2025-11-01' and '2025-12-01', exclusive of the upper bound), how many trips had a `trip_distance` of less than or equal to 1 mile?

- 7,853
- 8,007
- 8,254
- 8,421

### Solution:
```sql
SELECT COUNT(*) AS short_trips_Nov2025
  FROM public.green_tripdata_2025_11
 WHERE lpep_pickup_datetime >= '2025-11-01' 
   AND lpep_pickup_datetime <  '2025-12-01'
   AND trip_distance <= 1;
```
8,007 trips

## Question 4. Longest trip for each day

Which was the pick up day with the longest trip distance? Only consider trips with `trip_distance` less than 100 miles (to exclude data errors).

Use the pick up time for your calculations.

- 2025-11-14
- 2025-11-20
- 2025-11-23
- 2025-11-25

### Solution:
```sql
SELECT lpep_pickup_datetime::date AS day
  FROM public.green_tripdata_2025_11
 WHERE trip_distance < 100
 ORDER BY trip_distance DESC
 LIMIT 1;
```
2025-11-14

## Question 5. Biggest pickup zone

Which was the pickup zone with the largest `total_amount` (sum of all trips) on November 18th, 2025?

- East Harlem North
- East Harlem South
- Morningside Heights
- Forest Hills

### Solution:
```sql
SELECT zones."Zone",
       SUM(trips.total_amount) OVER (PARTITION BY zones."Zone") AS total_amount
  FROM public.green_tripdata_2025_11 trips
  JOIN public.taxi_zone_lookup zones
    ON trips."PULocationID" = zones."LocationID"
 ORDER BY 2 DESC
 LIMIT 1;
```
East Harlem North

## Question 6. Largest tip

For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip?

Note: it's `tip` , not `trip`. We need the name of the zone, not the ID.

- JFK Airport
- Yorkville West
- East Harlem North
- LaGuardia Airport

### Solution:
```sql
SELECT do_zones."Zone",
       trips.tip_amount
  FROM public.green_tripdata_2025_11 AS trips
  JOIN public.taxi_zone_lookup AS pu_zones ON trips."PULocationID" = pu_zones."LocationID"
  JOIN public.taxi_zone_lookup AS do_zones ON trips."DOLocationID" = do_zones."LocationID"
 WHERE pu_zones."Zone" = 'East Harlem North'
   AND trips.lpep_pickup_datetime >= '2025-11-01' 
   AND trips.lpep_pickup_datetime <  '2025-12-01'
 ORDER BY 2 DESC
 LIMIT 1;
```
Yorkville West

## Terraform

In this section homework we'll prepare the environment by creating resources in GCP with Terraform.

In your VM on GCP/Laptop/GitHub Codespace install Terraform.
Copy the files from the course repo
[here](../../../01-docker-terraform/terraform/terraform) to your VM/Laptop/GitHub Codespace.

Modify the files as necessary to create a GCP Bucket and Big Query Dataset.


## Question 7. Terraform Workflow

Which of the following sequences, respectively, describes the workflow for:
1. Downloading the provider plugins and setting up backend,
2. Generating proposed changes and auto-executing the plan
3. Remove all resources managed by terraform`

Answers:
- terraform import, terraform apply -y, terraform destroy
- teraform init, terraform plan -auto-apply, terraform rm
- terraform init, terraform run -auto-approve, terraform destroy
- terraform init, terraform apply -auto-approve, terraform destroy
- terraform import, terraform apply -y, terraform rm

### Solution:
terraform init, terraform apply -auto-approve, terraform destroy
