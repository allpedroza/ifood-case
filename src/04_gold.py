"""Camada Gold de consumo das viagens Yellow da NYC TLC."""

import json
from pathlib import Path

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
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

YELLOW_CONTRACT = json.loads(
    (CONTRACT_PATH / "yellow.yml").read_text(encoding="utf-8")
)

GOLD_COLUMNS = [
    ("VendorID", "vendor_id", LongType()),
    ("passenger_count", "passenger_count", DoubleType()),
    ("total_amount", "total_amount", DoubleType()),
    ("tpep_pickup_datetime", "tpep_pickup_datetime", TimestampType()),
    ("tpep_dropoff_datetime", "tpep_dropoff_datetime", TimestampType()),
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
    # ("_reference_month", "_reference_month", StringType()),
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
