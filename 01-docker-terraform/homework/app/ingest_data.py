#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import sys
import click
import pyarrow.parquet as pq
from urllib.parse import urlparse
from pathlib import Path
import fsspec

from my_dtypes import DATASET_DTYPES

def process_parquet(
        url,
        name,
        chunksize=int(1e5),
        assert_dtypes=False,
        dtypes_map=None
        ):
    with fsspec.open(url, "rb", block_size=8 * 1024 * 1024, cache_type="readahead") as f:
        pf = pq.ParquetFile(f)
        for batch in pf.iter_batches(batch_size=chunksize):
            df = batch.to_pandas()
            if assert_dtypes:
                if dtypes_map is None:
                    raise ValueError("No dtypes object was provided.")
                spec = dtypes_map[name]
                df[spec["DATETIME_COLS"]] = df[spec["DATETIME_COLS"]].apply(pd.to_datetime)
                df = df.astype(spec["DTYPES"])
            yield df

@click.command()
@click.option('--pg-user', envvar='PG_USER', default='root', help='PostgreSQL user')
@click.option('--pg-pass', envvar='PG_PASS', default='root', help='PostgreSQL password')
@click.option('--pg-host', envvar='PG_HOST', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', envvar='PG_PORT', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', envvar='PG_DB', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
# @click.option('--year', default=None, type=int, help='collection year')
# @click.option('--month', default=None, type=int, help='collection year')
@click.option('--chunksize', envvar='CHUNKSIZE', default=100000, type=int, help='chunk size for reading file')
@click.option('--url', default="https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet", help='URL to file')

def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, chunksize, url): # year, month, 
    # prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    # url = f'{prefix}yellow_tripdata_{year}-{month:02d}.csv.gz'

    engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    path_str = urlparse(url).path
    filename = Path(path_str).name
    extension = Path(filename).suffixes
    name = Path(filename).stem

    if '.csv' in extension:
        df_iter = pd.read_csv(
            url,
            dtype=DATASET_DTYPES[name]['DTYPES'],
            parse_dates=DATASET_DTYPES[name]['DATETIME_COLS'],
            iterator=True,
            chunksize=chunksize
        )
    elif extension[0] == '.parquet':
        df_iter = process_parquet(
            url=url,
            name = name,
            chunksize=chunksize,
            assert_dtypes=False,
            dtypes_map=DATASET_DTYPES,
        )
    else:
        print("Invalid file. Terminating.")
        sys.exit(1)

    first = True

    for df_chunk in tqdm(df_iter):

        if first:
            # Create table schema (no data)
            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists="replace"
            )
            first = False
            # print("Table created")

        # Insert chunk
        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append"
        )
    print("\n" + "="*60)
    print("Data ingestion complete!")
    print("="*60 + "\n")
    print("You can now connect to pgAdmin at http://localhost:8080")
    print("Email: admin@admin.com")
    print(f"Pswd: {pg_pass}")
    print(f"Username: {pg_user}")
    print(f"Port: {pg_port}")
    


if __name__ == '__main__':
    run()
    
