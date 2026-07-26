"""Camada Gold de consumo das viagens Yellow da NYC TLC."""

from datetime import datetime
import json
from pathlib import Path

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


SILVER_CATALOG = spark.conf.get("ifood.silver_catalog")
SILVER_SCHEMA = spark.conf.get("ifood.silver_schema")
GOLD_CATALOG = spark.conf.get("ifood.gold_catalog")
GOLD_SCHEMA = spark.conf.get("ifood.gold_schema")
CONTRACT_PATH = Path(spark.conf.get("ifood.metadata_contract_path"))
START_MONTH = spark.conf.get("ifood.silver.start_month")
END_MONTH = spark.conf.get("ifood.silver.end_month")
PASSENGER_REFERENCE_MONTH = spark.conf.get(
    "ifood.analysis.passenger_reference_month"
)


def load_contract(name: str):
    return json.loads(
        (CONTRACT_PATH / f"{name}.yml").read_text(encoding="utf-8")
    )


YELLOW_CONTRACT = load_contract("yellow")
GREEN_CONTRACT = load_contract("green")

GOLD_COLUMNS = [
    ("VendorID", "vendor_id", LongType()),
    ("passenger_count", "passenger_count", DoubleType()),
    ("total_amount", "total_amount", DoubleType()),
    ("tpep_pickup_datetime", "tpep_pickup_datetime", TimestampType()),
    ("tpep_dropoff_datetime", "tpep_dropoff_datetime", TimestampType()),
    ("_reference_month", "_reference_month", StringType()),
    # ("trip_distance", "trip_distance", DoubleType()),
    # ("RatecodeID", "ratecode_id", DoubleType()),
    # ("store_and_fwd_flag", "store_and_fwd_flag", StringType()),
    # ("PULocationID", "pu_location_id", LongType()),
    # ("DOLocationID", "do_location_id", LongType()),
    # ("payment_type", "payment_type", LongType()),
    # ("fare_amount", "fare_amount", DoubleType()),
    # ("extra", "extra", DoubleType()),
    # ("mta_tax", "mta_tax", DoubleType()),
    # ("tip_amount", "tip_amount", DoubleType()),
    # ("tolls_amount", "tolls_amount", DoubleType()),
    # ("improvement_surcharge", "improvement_surcharge", DoubleType()),
    # ("congestion_surcharge", "congestion_surcharge", DoubleType()),
    # ("airport_fee", "airport_fee", DoubleType()),
    # ("cbd_congestion_fee", "cbd_congestion_fee", DoubleType()),
    # Colunas técnicas de linhagem disponíveis na Silver.
    # ("_source_file", "_source_file", StringType()),
    # ("_ingested_at", "_ingested_at", TimestampType()),
    # ("_service_type", "_service_type", StringType()),
]

TECHNICAL_COLUMN_DESCRIPTIONS = {
    "_source_file": "Caminho do arquivo que originou o registro na Landing.",
    "_ingested_at": "Instante em que o registro foi processado na Bronze.",
    "_service_type": "Categoria de serviço da NYC TLC.",
    "_reference_month": "Mês de referência da viagem.",
}


def gold_column_description(target: str) -> str:
    contract_column = YELLOW_CONTRACT["columns"].get(target)
    if contract_column:
        return contract_column["description"]
    return TECHNICAL_COLUMN_DESCRIPTIONS[target]


GOLD_SCHEMA_DEFINITION = StructType(
    [
        StructField(
            target,
            data_type,
            True,
            {
                "comment": gold_column_description(target),
                "source_column": silver_source,
                "inherited_from": "silver_yellow_trips",
                "official_dictionary": YELLOW_CONTRACT["source_file"],
                "source_sha256": YELLOW_CONTRACT["source_sha256"],
            },
        )
        for target, silver_source, data_type in GOLD_COLUMNS
    ]
)


@dp.materialized_view(
    name=f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_yellow_trips_consumption",
    schema=GOLD_SCHEMA_DEFINITION,
    comment=(
        f"Consumption-ready Yellow Taxi trips from {START_MONTH} through "
        f"{END_MONTH}, "
        "with metadata inherited from the deterministic official contract."
    ),
    table_properties={
        "quality": "gold",
        "tlc.metadata.source_layer": "silver",
        "tlc.metadata.contract_language": "en",
        "tlc.metadata.contract_sha256": YELLOW_CONTRACT["source_sha256"],
    },
)
def gold_yellow_trips_consumption():
    source = spark.read.table(
        f"`{SILVER_CATALOG}`.`{SILVER_SCHEMA}`.`silver_yellow_trips`"
    )
    return source.select(
        *[
            F.col(silver_source).cast(data_type).alias(target)
            for target, silver_source, data_type in GOLD_COLUMNS
        ]
    )


def month_bounds(reference_month: str):
    start = datetime.strptime(reference_month, "%Y-%m")
    if start.month == 12:
        end = start.replace(year=start.year + 1, month=1)
    else:
        end = start.replace(month=start.month + 1)
    return start, end


PASSENGER_MONTH_START, PASSENGER_MONTH_END = month_bounds(
    PASSENGER_REFERENCE_MONTH
)

PASSENGER_BY_HOUR_SCHEMA = StructType(
    [
        StructField(
            "hour_of_day",
            IntegerType(),
            False,
            {
                "comment": "Hour of pickup, from 0 through 23.",
                "source_columns": (
                    "tpep_pickup_datetime,lpep_pickup_datetime"
                ),
            },
        ),
        StructField(
            "average_passenger_count",
            DoubleType(),
            True,
            {
                "comment": (
                    "Average positive passenger count across Yellow and "
                    "Green Taxi trips."
                ),
                "source_column": "passenger_count",
            },
        ),
        StructField(
            "trips_considered",
            LongType(),
            False,
            {
                "comment": (
                    "Trips with passenger_count greater than zero included "
                    "in the average."
                ),
            },
        ),
        StructField(
            "yellow_trips_considered",
            LongType(),
            False,
            {
                "comment": "Yellow Taxi trips included in the average.",
            },
        ),
        StructField(
            "green_trips_considered",
            LongType(),
            False,
            {
                "comment": "Green Taxi trips included in the average.",
            },
        ),
        StructField(
            "trips_discarded",
            LongType(),
            False,
            {
                "comment": (
                    "Trips excluded because passenger_count is null or "
                    "less than or equal to zero."
                ),
            },
        ),
    ]
)


@dp.materialized_view(
    name=f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_taxi_passengers_by_hour",
    schema=PASSENGER_BY_HOUR_SCHEMA,
    comment=(
        f"Average passengers by pickup hour for Yellow and Green Taxi trips "
        f"in {PASSENGER_REFERENCE_MONTH}. FHV and High Volume FHV are "
        "excluded because they are not taxi services and do not provide "
        "passenger_count."
    ),
    table_properties={
        "quality": "gold",
        "tlc.metadata.source_layer": "silver",
        "tlc.metadata.service_scope": "yellow,green",
        "tlc.metadata.passenger_filter": "passenger_count > 0",
        "tlc.metadata.reference_month": PASSENGER_REFERENCE_MONTH,
        "tlc.metadata.yellow_contract_sha256": YELLOW_CONTRACT["source_sha256"],
        "tlc.metadata.green_contract_sha256": GREEN_CONTRACT["source_sha256"],
    },
)
def gold_taxi_passengers_by_hour():
    yellow = spark.read.table(
        f"`{SILVER_CATALOG}`.`{SILVER_SCHEMA}`.`silver_yellow_trips`"
    ).select(
        F.col("tpep_pickup_datetime").alias("pickup_datetime"),
        "passenger_count",
        F.lit("yellow").alias("service_type"),
    )
    green = spark.read.table(
        f"`{SILVER_CATALOG}`.`{SILVER_SCHEMA}`.`silver_green_trips`"
    ).select(
        F.col("lpep_pickup_datetime").alias("pickup_datetime"),
        "passenger_count",
        F.lit("green").alias("service_type"),
    )
    trips = yellow.unionByName(green).filter(
        (F.col("pickup_datetime") >= F.lit(PASSENGER_MONTH_START))
        & (F.col("pickup_datetime") < F.lit(PASSENGER_MONTH_END))
    )
    valid = F.col("passenger_count") > 0
    discarded = F.col("passenger_count").isNull() | (
        F.col("passenger_count") <= 0
    )
    return (
        trips.withColumn("hour_of_day", F.hour("pickup_datetime"))
        .groupBy("hour_of_day")
        .agg(
            F.avg(F.when(valid, F.col("passenger_count"))).alias(
                "average_passenger_count"
            ),
            F.count(F.when(valid, 1)).alias("trips_considered"),
            F.count(
                F.when(valid & (F.col("service_type") == "yellow"), 1)
            ).alias("yellow_trips_considered"),
            F.count(
                F.when(valid & (F.col("service_type") == "green"), 1)
            ).alias("green_trips_considered"),
            F.count(F.when(discarded, 1)).alias("trips_discarded"),
        )
        .orderBy("hour_of_day")
    )
