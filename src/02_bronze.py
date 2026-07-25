"""Tabelas Bronze da NYC TLC em Lakeflow Spark Declarative Pipelines."""

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


LANDING_PATH = spark.conf.get("ifood.landing_path")
SCHEMA_LANDING_ESPERADO = "tlc_data_landing"
SCHEMA_BRONZE_ESPERADO = "tlc_data_bronze"
VOLUME_LANDING_ESPERADO = "nyc_tlc_landing"
METADATA_PATH = f"{LANDING_PATH}/_metadata"
METADATA_CONTRACT_PATH = Path(spark.conf.get("ifood.metadata_contract_path"))
TRIP_DATASETS = ("yellow", "green", "fhv", "fhvhv")
CONTRACT_DATASETS = (*TRIP_DATASETS, "taxi_zones")

URL_GUIA = "https://home4.nyc.gov/assets/tlc/downloads/pdf/trip_record_user_guide.pdf"
URL_PARQUET = "https://home4.nyc.gov/assets/tlc/downloads/pdf/working_parquet_format.pdf"
URL_DICIONARIOS = {
    "yellow": "https://home4.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf",
    "green": "https://home4.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf",
    "fhv": "https://home4.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_fhv.pdf",
    # O slug oficial do dicionário é hvfhs; o dataset mensal usa fhvhv.
    "fhvhv": "https://home4.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_hvfhs.pdf",
}

ARTEFATOS_METADATA = {
    "trip_record_user_guide.pdf": ("guia", "todos", "trip_records", URL_GUIA),
    "data_dictionary_trip_records_yellow.pdf": (
        "dicionario",
        "yellow",
        "yellow",
        URL_DICIONARIOS["yellow"],
    ),
    "data_dictionary_trip_records_green.pdf": (
        "dicionario",
        "green",
        "green",
        URL_DICIONARIOS["green"],
    ),
    "data_dictionary_trip_records_fhv.pdf": (
        "dicionario",
        "fhv",
        "fhv",
        URL_DICIONARIOS["fhv"],
    ),
    "data_dictionary_trip_records_hvfhs.pdf": (
        "dicionario",
        "fhvhv",
        "hvfhs",
        URL_DICIONARIOS["fhvhv"],
    ),
    "working_parquet_format.pdf": ("nota_tecnica", "todos", "parquet", URL_PARQUET),
    "taxi_zone_lookup.csv": (
        "lookup_geografica",
        "todos",
        "taxi_zones",
        "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv",
    ),
    "taxi_zones.zip": (
        "shapefile",
        "todos",
        "taxi_zones",
        "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip",
    ),
}


def carregar_contratos():
    """Carrega contratos YAML 1.2 escritos como JSON para dispensar dependências."""
    contratos = {}
    for nome in CONTRACT_DATASETS:
        caminho = METADATA_CONTRACT_PATH / f"{nome}.yml"
        contratos[nome] = json.loads(caminho.read_text(encoding="utf-8"))
    return contratos


CONTRATOS = carregar_contratos()
COLUNAS_TECNICAS = {
    "_source_file": {
        "description": "Path of the source file in the Landing volume.",
        "type": "string",
    },
    "_ingested_at": {
        "description": "Timestamp when the record was processed in Bronze.",
        "type": "timestamp",
    },
    "_service_type": {
        "description": "NYC TLC service category.",
        "type": "string",
    },
    "_reference_month": {
        "description": "Reference month extracted from the source file name.",
        "type": "string",
    },
}
COLUNA_RESCUE = {
    "_rescued_data": {
        "description": "Source fields not declared by the official contract.",
        "type": "string",
    }
}
TYPE_MAP = {
    "double": DoubleType(),
    "integer": IntegerType(),
    "long": LongType(),
    "string": StringType(),
    "timestamp": TimestampType(),
}


def validar_schemas():
    """Garante o padrão tlc_data_<layer> configurado no Bundle."""
    partes = LANDING_PATH.split("/")
    schema_landing = partes[3] if len(partes) > 3 else ""
    volume_landing = partes[4] if len(partes) > 4 else ""
    schema_bronze = spark.conf.get("ifood.bronze_schema")
    if schema_landing != SCHEMA_LANDING_ESPERADO:
        raise ValueError(
            f"schema Landing inválido: {schema_landing!r}; "
            f"esperado: {SCHEMA_LANDING_ESPERADO!r}"
        )
    if schema_bronze != SCHEMA_BRONZE_ESPERADO:
        raise ValueError(
            f"schema Bronze inválido: {schema_bronze!r}; "
            f"esperado: {SCHEMA_BRONZE_ESPERADO!r}"
        )
    if volume_landing != VOLUME_LANDING_ESPERADO:
        raise ValueError(
            f"Volume Landing inválido: {volume_landing!r}; "
            f"esperado: {VOLUME_LANDING_ESPERADO!r}"
        )


validar_schemas()


def definicoes_bronze(tipo: str):
    return (
        CONTRATOS[tipo]["columns"]
        | COLUNA_RESCUE
        | COLUNAS_TECNICAS
    )


def schema_bronze(tipo: str):
    contrato = CONTRATOS[tipo]
    return StructType(
        [
            StructField(
                nome,
                TYPE_MAP[metadado["type"]],
                True,
                {
                    "comment": metadado["description"],
                    "contract_source": contrato["source_file"],
                    "contract_sha256": contrato["source_sha256"],
                    "extractor_version": contrato["extractor_version"],
                },
            )
            for nome, metadado in definicoes_bronze(tipo).items()
        ]
    )


def ler_parquets(tipo: str):
    """Lê incrementalmente os Parquets brutos de uma categoria."""
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.includeExistingFiles", "true")
        .load(f"{LANDING_PATH}/{tipo}")
    )
    colunas_contrato = [
        F.col(nome).cast(TYPE_MAP[metadado["type"]]).alias(nome)
        for nome, metadado in CONTRATOS[tipo]["columns"].items()
    ]
    return df.select(
        *colunas_contrato,
        F.col("_rescued_data").cast("string").alias("_rescued_data"),
        F.col("_metadata.file_path").cast("string").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
        F.lit(tipo).cast("string").alias("_service_type"),
        F.regexp_extract(
            F.col("_metadata.file_name"),
            r"(\d{4}-\d{2})",
            1,
        ).alias("_reference_month"),
    )


def propriedades_tabela(tipo: str):
    propriedades = {
        "tlc.metadata.user_guide": URL_GUIA,
        "tlc.metadata.data_dictionary": URL_DICIONARIOS[tipo],
        "tlc.metadata.parquet_format": URL_PARQUET,
        "tlc.metadata.dataset_slug": tipo,
    }
    if tipo == "fhvhv":
        propriedades["tlc.metadata.dictionary_slug"] = "hvfhs"
        propriedades["tlc.metadata.slug_mapping"] = "hvfhs -> fhvhv"
    return propriedades


@dp.table(
    name="bronze_yellow_trips",
    schema=schema_bronze("yellow"),
    comment=CONTRATOS["yellow"]["description"],
    table_properties=propriedades_tabela("yellow"),
)
def yellow_trips():
    return ler_parquets("yellow")


@dp.table(
    name="bronze_green_trips",
    schema=schema_bronze("green"),
    comment=CONTRATOS["green"]["description"],
    table_properties=propriedades_tabela("green"),
)
def green_trips():
    return ler_parquets("green")


@dp.table(
    name="bronze_fhv_trips",
    schema=schema_bronze("fhv"),
    comment=CONTRATOS["fhv"]["description"],
    table_properties=propriedades_tabela("fhv"),
)
def fhv_trips():
    return ler_parquets("fhv")


@dp.table(
    name="bronze_fhvhv_trips",
    schema=schema_bronze("fhvhv"),
    comment=CONTRATOS["fhvhv"]["description"],
    table_properties=propriedades_tabela("fhvhv"),
)
def fhvhv_trips():
    return ler_parquets("fhvhv")


@dp.materialized_view(
    name="bronze_metadata_artifacts",
    comment=(
        "Inventário governado dos documentos e arquivos geográficos oficiais "
        "da NYC TLC armazenados na Landing."
    ),
)
def metadata_artifacts():
    df = spark.read.format("binaryFile").load(f"{METADATA_PATH}/*")
    metadados = []
    for nome, (tipo, dataset, slug_origem, url) in ARTEFATOS_METADATA.items():
        metadados.extend(
            [
                F.lit(nome),
                F.struct(
                    F.lit(tipo).alias("artifact_type"),
                    F.lit(dataset).alias("dataset_slug"),
                    F.lit(slug_origem).alias("source_slug"),
                    F.lit(url).alias("source_url"),
                ),
            ]
        )
    mapa = F.create_map(*metadados)
    nome_arquivo = F.regexp_extract(F.col("path"), r"([^/]+)$", 1)
    return (
        df.withColumn("file_name", nome_arquivo)
        .withColumn("_governance", F.element_at(mapa, F.col("file_name")))
        .select(
            "file_name",
            F.col("_governance.artifact_type").alias("artifact_type"),
            F.col("_governance.dataset_slug").alias("dataset_slug"),
            F.col("_governance.source_slug").alias("source_slug"),
            F.col("_governance.source_url").alias("source_url"),
            F.col("path").alias("landing_path"),
            F.col("length").alias("size_bytes"),
            F.col("modificationTime").alias("modified_at"),
            F.when(
                (F.col("file_name") == "data_dictionary_trip_records_hvfhs.pdf"),
                F.lit("The official dictionary uses hvfhs; the dataset uses fhvhv."),
            ).alias("governance_note"),
        )
    )


TAXI_ZONE_SOURCE_SCHEMA = StructType(
    [
        StructField(
            "LocationID",
            IntegerType(),
            False,
        ),
        StructField("Borough", StringType(), True),
        StructField("Zone", StringType(), True),
        StructField("service_zone", StringType(), True),
    ]
)

TAXI_ZONE_SCHEMA = StructType(
    [
        StructField(
            nome,
            TYPE_MAP[metadado["type"]],
            True,
            {
                "comment": metadado["description"],
                "contract_source": CONTRATOS["taxi_zones"]["source_file"],
                "contract_sha256": CONTRATOS["taxi_zones"]["source_sha256"],
                "extractor_version": CONTRATOS["taxi_zones"]["extractor_version"],
            },
        )
        for nome, metadado in CONTRATOS["taxi_zones"]["columns"].items()
    ]
    + [
        StructField(
            "_source_file",
            StringType(),
            False,
            {"comment": "Path of the source lookup file in Landing."},
        ),
        StructField(
            "_ingested_at",
            TimestampType(),
            False,
            {"comment": "Timestamp when the lookup was processed in Bronze."},
        ),
    ]
)


@dp.materialized_view(
    name="bronze_taxi_zone_lookup",
    schema=TAXI_ZONE_SCHEMA,
    comment=(
        CONTRATOS["taxi_zones"]["description"]
    ),
    table_properties={
        "tlc.metadata.join_keys": "PULocationID,DOLocationID -> LocationID",
        "tlc.metadata.source": "NYC TLC taxi_zone_lookup.csv",
    },
)
def taxi_zone_lookup():
    return (
        spark.read.option("header", "true")
        .schema(TAXI_ZONE_SOURCE_SCHEMA)
        .csv(f"{METADATA_PATH}/taxi_zone_lookup.csv")
        .withColumn(
            "_source_file",
            F.lit(f"{METADATA_PATH}/taxi_zone_lookup.csv"),
        )
        .withColumn("_ingested_at", F.current_timestamp())
    )


@dp.materialized_view(
    name="bronze_metadata_columns",
    comment=(
        "Cobertura determinística dos contratos de colunas: documentada, "
        "não documentada ou ausente na origem."
    ),
)
def metadata_columns():
    linhas = []
    for tipo in TRIP_DATASETS:
        campos_origem = {
            campo.name.lower(): (campo.name, campo.dataType.simpleString())
            for campo in spark.read.table(CONTRATOS[tipo]["table"]).schema.fields
        }
        contrato = CONTRATOS[tipo]
        campos_contrato = {
            nome.lower(): (nome, metadado)
            for nome, metadado in (
                contrato["columns"] | COLUNAS_TECNICAS
            ).items()
        }
        for chave in sorted(campos_origem.keys() | campos_contrato.keys()):
            origem = campos_origem.get(chave)
            esperado = campos_contrato.get(chave)
            if origem and esperado:
                status = "DOCUMENTED"
            elif origem:
                status = "UNDOCUMENTED"
            else:
                status = "MISSING_IN_SOURCE"
            linhas.append(
                (
                    tipo,
                    contrato["table"],
                    origem[0] if origem else esperado[0],
                    origem[1] if origem else None,
                    esperado[1].get("type") if esperado else None,
                    esperado[1].get("description") if esperado else None,
                    esperado[1].get("references") if esperado else None,
                    status,
                    contrato["dictionary_slug"],
                    URL_DICIONARIOS[tipo],
                )
            )
    return spark.createDataFrame(
        linhas,
        (
            "dataset string, table_name string, column_name string, "
            "actual_type string, expected_type string, description string, "
            "references string, status string, dictionary_slug string, "
            "dictionary_url string"
        ),
    )
