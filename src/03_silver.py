"""Tabelas Silver padronizadas da NYC TLC para janeiro a maio de 2023."""

import json
import re
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


BRONZE_CATALOG = spark.conf.get("ifood.bronze_catalog")
BRONZE_SCHEMA = spark.conf.get("ifood.bronze_schema")
SILVER_CATALOG = spark.conf.get("ifood.silver_catalog")
SILVER_SCHEMA = spark.conf.get("ifood.silver_schema")
CONTRACT_PATH = Path(spark.conf.get("ifood.metadata_contract_path"))
START_MONTH = spark.conf.get("ifood.silver.start_month")
END_MONTH = spark.conf.get("ifood.silver.end_month")
TRIP_DATASETS = ("yellow", "green", "fhv", "fhvhv")
CONTRACT_DATASETS = (*TRIP_DATASETS, "taxi_zones")

TYPE_MAP = {
    "double": DoubleType(),
    "integer": IntegerType(),
    "long": LongType(),
    "string": StringType(),
    "timestamp": TimestampType(),
}

TECHNICAL_COLUMNS = {
    "_source_file": {
        "description": "Caminho do arquivo que originou o registro na Landing.",
        "type": "string",
    },
    "_ingested_at": {
        "description": "Instante em que o registro foi processado na Bronze.",
        "type": "timestamp",
    },
    "_service_type": {
        "description": "Categoria de serviço da NYC TLC.",
        "type": "string",
    },
    "_reference_month": {
        "description": "Mês de referência da viagem.",
        "type": "string",
    },
}


def load_contract(name: str):
    return json.loads(
        (CONTRACT_PATH / f"{name}.yml").read_text(encoding="utf-8")
    )


CONTRACTS = {
    name: load_contract(name)
    for name in CONTRACT_DATASETS
}


def snake_case(name: str) -> str:
    special = {
        "vendorid": "vendor_id",
        "ratecodeid": "ratecode_id",
        "pulocationid": "pu_location_id",
        "dolocationid": "do_location_id",
    }
    if name.lower() in special:
        return special[name.lower()]
    value = re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()
    return re.sub(r"_+", "_", value)


def bronze_table(contract):
    return (
        f"`{BRONZE_CATALOG}`.`{BRONZE_SCHEMA}`."
        f"`{contract['table']}`"
    )


def silver_definitions(dataset: str):
    contract = CONTRACTS[dataset]
    definitions = dict(contract["columns"])
    if dataset != "taxi_zones":
        definitions.update(TECHNICAL_COLUMNS)
    return contract, definitions


def silver_schema(dataset: str):
    contract, definitions = silver_definitions(dataset)
    fields = []
    for contract_name, metadata in definitions.items():
        target_name = snake_case(contract_name)
        target_type = TYPE_MAP[metadata["type"]]
        column_metadata = {
            "comment": metadata["description"],
            "source_column": contract_name,
            "inherited_from": contract["table"],
        }
        if metadata.get("references"):
            column_metadata["references"] = metadata["references"]
        fields.append(
            StructField(
                target_name,
                target_type,
                True,
                column_metadata,
            )
        )
    return StructType(fields)


def silver_query(dataset: str):
    contract, definitions = silver_definitions(dataset)
    source = spark.read.table(bronze_table(contract))
    columns = [
        F.col(source_name)
        .cast(TYPE_MAP[metadata["type"]])
        .alias(snake_case(source_name))
        for source_name, metadata in definitions.items()
    ]
    if dataset == "taxi_zones":
        return source.select(*columns)
    return source.filter(
        F.col("_reference_month").between(START_MONTH, END_MONTH)
    ).select(*columns)


def silver_comment(dataset_name: str) -> str:
    return (
        f"Viagens {dataset_name} tratadas entre {START_MONTH} e {END_MONTH}, "
        "com metadados herdados do contrato Bronze."
    )


@dp.materialized_view(
    name=f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_yellow_trips",
    schema=silver_schema("yellow"),
    comment=silver_comment("Yellow"),
)
def silver_yellow_trips():
    return silver_query("yellow")


@dp.materialized_view(
    name=f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_green_trips",
    schema=silver_schema("green"),
    comment=silver_comment("Green"),
)
def silver_green_trips():
    return silver_query("green")


@dp.materialized_view(
    name=f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_fhv_trips",
    schema=silver_schema("fhv"),
    comment=silver_comment("FHV"),
)
def silver_fhv_trips():
    return silver_query("fhv")


@dp.materialized_view(
    name=f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_fhvhv_trips",
    schema=silver_schema("fhvhv"),
    comment=(
        f"Viagens High Volume FHV tratadas entre {START_MONTH} e {END_MONTH}, "
        "com de-para hvfhs para fhvhv preservado."
    ),
    table_properties={
        "tlc.metadata.dictionary_slug": "hvfhs",
        "tlc.metadata.dataset_slug": "fhvhv",
        "tlc.metadata.slug_mapping": "hvfhs -> fhvhv",
    },
)
def silver_fhvhv_trips():
    return silver_query("fhvhv")


@dp.materialized_view(
    name=f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_taxi_zone_lookup",
    schema=silver_schema("taxi_zones"),
    comment="Lookup geográfica oficial tratada e herdada da Bronze.",
)
def silver_taxi_zone_lookup():
    return silver_query("taxi_zones")
