import os
from datetime import datetime

import duckdb
import polars as pl
from dagster import (
    AssetExecutionContext,
    AutoMaterializePolicy,
    Definitions,
    MonthlyPartitionsDefinition,
    asset,
)

NYC_TAXI_URL = os.environ["NYC_TAXI_URL"]
DUCKDB_TAXI_PATH = os.environ["DUCKDB_TAXI_PATH"]

monthly_partition_def = MonthlyPartitionsDefinition(start_date="2020-01-01")


@asset(partitions_def=monthly_partition_def, compute_kind="polars")
def yellow_trip_parquet(context: AssetExecutionContext) -> pl.DataFrame:
    pdate = datetime.strptime(context.partition_key, "%Y-%m-%d")
    path = f"{NYC_TAXI_URL}/yellow_tripdata_{pdate.year}-{pdate.month:02}.parquet"
    data = pl.read_parquet(path)

    context.log.info("Load NYC yellow taxi trip data")
    context.log.info(f"Partition Key: {context.partition_key}")
    context.add_output_metadata({"num_rows": data.shape[0]})

    return data


@asset(
    partitions_def=monthly_partition_def,
    auto_materialize_policy=AutoMaterializePolicy.eager(),
    compute_kind="polars",
    tags={"dagster/storage_kind": "duckdb"},
)
def yellow_trip_table(
    context: AssetExecutionContext, yellow_trip_parquet: pl.DataFrame
) -> None:
    data = yellow_trip_parquet

    conn = duckdb.connect(os.fspath(DUCKDB_TAXI_PATH))
    # need to make sure database is initialized with a schema and table
    conn.execute("INSERT INTO nyc_taxi.raw_yellow_trips SELECT * FROM data;")

    context.log.info("Load NYC yellow taxi trip data")
    context.log.info(f"Partition Key: {context.partition_key}")
    context.add_output_metadata({"num_rows": data.shape[0]})


defs = Definitions(
    assets=[yellow_trip_parquet, yellow_trip_table],
)
