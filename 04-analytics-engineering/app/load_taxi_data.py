import os
import sys
from itertools import starmap, product
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage, bigquery
from google.api_core.exceptions import NotFound, Forbidden
import time
import socket
import requests


# Change this to your bucket name
BUCKET_NAME = "de_zoomcamp_hw4_2026_andres"

# If you authenticated through the GCP SDK you can comment out these two lines
# CREDENTIALS_FILE = "/home/andres/.gcp/warehousing_hw3_key.json"
# client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
# If commented initialize client with the following
client = storage.Client() # project='zoomcamp-mod3-datawarehouse')


# BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/"
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"

COLORS = ["yellow", "green"]
YEARS = [i for i in range(2019, 2020+1)]
MONTHS = [f"{i:02d}" for i in range(1, 12+1)]
DOWNLOAD_DIR = "."

CHUNK_SIZE = 8 * 1024 * 1024

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

bucket = client.bucket(BUCKET_NAME)
bq_client = bigquery.Client()

session = requests.Session()

def download_file(color, year, month):
    url = f"{BASE_URL}{color}/{color}_tripdata_{year}-{month}.csv.gz"
    file_path = os.path.join(DOWNLOAD_DIR, f"{color}_tripdata_{year}-{month}.csv.gz")

    try:
        print(f"Downloading {url}...", flush=True)
        # stream=True is critical for large Parquet files
        with session.get(url, stream=True, timeout=30) as r:
            r.raise_for_status()
            with open(file_path, 'wb') as f:
                # Use a large chunk size (e.g., 1MB) for faster throughput
                for chunk in r.iter_content(chunk_size=1024*1024): 
                    f.write(chunk)
        print(f"Downloaded: {file_path}", flush=True)
        return file_path
    except Exception as e:
        print(f"Failed to download {url}: {e}", flush=True)
        return None

def create_bucket(bucket_name):
    try:
        # Get bucket details
        bucket = client.get_bucket(bucket_name)

        # Check if the bucket belongs to the current project
        project_bucket_ids = [bckt.id for bckt in client.list_buckets()]
        if bucket_name in project_bucket_ids:
            print(
                f"Bucket '{bucket_name}' exists and belongs to your project. Proceeding..."
            )
        else:
            print(
                f"A bucket with the name '{bucket_name}' already exists, but it does not belong to your project."
            )
            sys.exit(1)

    except NotFound:
        # If the bucket doesn't exist, create it
        bucket = client.create_bucket(bucket_name)
        print(f"Created bucket '{bucket_name}'")
    except Forbidden:
        # If the request is forbidden, it means the bucket exists but you don't have access to see details
        print(
            f"A bucket with the name '{bucket_name}' exists, but it is not accessible. Bucket name is taken. Please try a different bucket name."
        )
        sys.exit(1)


def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)


def upload_to_gcs(file_path, max_retries=3):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    # create_bucket(BUCKET_NAME)

    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")

            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                os.remove(file_path)
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")

        time.sleep(5)

    print(f"Giving up on {file_path} after {max_retries} attempts.")

def generate_query(color):
    query = f"""
        LOAD DATA OVERWRITE `dtc-de-course-491308.nytaxi.{color}_tripdata`
        FROM FILES (
          format = 'CSV',
          uris = ['gs://{BUCKET_NAME}/{color}_tripdata_*.csv.gz'],
          compression = 'GZIP',
          skip_leading_rows = 1,
          quote = '"',
          allow_quoted_newlines = True,
          ignore_unknown_values = True,
          allow_jagged_rows = True
        );
    """
    return query


def upload_to_bigquery(query):
    try:
        print(f"Executing BQ Query...", flush=True)
        query_job = bq_client.query(query)
        results = query_job.result() # This waits for the job to finish
        print(f"BQ Job Finished: {query_job.job_id}", flush=True)
    except Exception as e:
        print(f"BQ Error: {e}", flush=True)
    # query_job = bq_client.query(query)
    # results = query_job.result()

def create_dataset():
    dataset_id = "dtc-de-course-491308.nytaxi"
    try:
        bq_client.get_dataset(dataset_id)
        print(f"Dataset {dataset_id} already exists.")
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"  # Ensure this matches your GCS bucket location
        bq_client.create_dataset(dataset, timeout=30)
        print(f"Created dataset {dataset_id}")

if __name__ == "__main__":
    create_bucket(BUCKET_NAME)
    create_dataset()
    args = list(product(COLORS, YEARS, MONTHS))

    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(lambda p: download_file(*p), args))

    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, filter(None, file_paths))  # Remove None values

    with ThreadPoolExecutor(max_workers=4) as executor:
        queries = list(executor.map(generate_query, COLORS))

    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_bigquery, queries)

    print("All files processed and verified.")
