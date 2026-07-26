# Databricks notebook source
# MAGIC %md
# MAGIC # Profiling da camada Gold
# MAGIC
# MAGIC Este notebook examina a natureza, distribuição, ausência, cardinalidade
# MAGIC e correlação dos atributos publicados nas duas tabelas de consumo:
# MAGIC
# MAGIC - `gold_yellow_trips_consumption`: viagens Yellow em granularidade
# MAGIC   detalhada;
# MAGIC - `gold_taxi_passengers_by_hour`: agregado horário de Yellow e Green.
# MAGIC
# MAGIC As métricas gerais são calculadas sobre a tabela completa com Spark. O
# MAGIC relatório detalhado converte somente uma amostra controlada para Pandas,
# MAGIC evitando carregar toda a Gold na memória do processo Python.

# COMMAND ----------

# MAGIC %pip install fg-data-profiling==4.19.1

# COMMAND ----------

from data_profiling import ProfileReport
from pyspark.sql import functions as F


RANDOM_SEED = 42
GOLD_TABLES = [
    {
        "table": (
            "case_ifood.tlc_data_gold."
            "gold_yellow_trips_consumption"
        ),
        "title": "Gold Yellow Taxi — viagens",
        "range_columns": [
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
        ],
        "sample": True,
    },
    {
        "table": (
            "case_ifood.tlc_data_gold."
            "gold_taxi_passengers_by_hour"
        ),
        "title": "Gold Yellow + Green — passageiros por hora",
        "range_columns": [
            "hour_of_day",
            "average_passenger_count",
        ],
        "sample": False,
    },
]

dbutils.widgets.text("sample_fraction", "0.01", "Fração da amostra")
dbutils.widgets.text("max_rows", "100000", "Máximo de registros no Pandas")

requested_fraction = float(dbutils.widgets.get("sample_fraction"))
max_rows = int(dbutils.widgets.get("max_rows"))

if not 0 < requested_fraction <= 1:
    raise ValueError("sample_fraction deve estar no intervalo (0, 1].")
if max_rows <= 0:
    raise ValueError("max_rows deve ser maior que zero.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execução do profiling
# MAGIC
# MAGIC Para cada tabela, o notebook apresenta schema, metadados e cobertura
# MAGIC completa com Spark. A tabela detalhada usa amostra controlada no Pandas;
# MAGIC a tabela horária possui apenas 24 linhas e é perfilada integralmente.

# COMMAND ----------

def coverage_expressions(dataframe, range_columns):
    expressions = [
        F.count(F.lit(1)).alias("total_rows"),
        *[
            F.count(F.when(F.col(column).isNull(), 1)).alias(
                f"{column}__null_count"
            )
            for column in dataframe.columns
        ],
    ]
    for column in range_columns:
        expressions.extend(
            [
                F.min(column).alias(f"{column}__min"),
                F.max(column).alias(f"{column}__max"),
            ]
        )
    return expressions


def pandas_dataframe(dataframe, total_rows, use_sample):
    if total_rows == 0:
        raise ValueError("A tabela Gold não possui registros.")
    if not use_sample:
        return dataframe.toPandas(), 1.0

    safe_fraction = min(requested_fraction, max_rows / total_rows)
    sampled = (
        dataframe.sample(
            withReplacement=False,
            fraction=safe_fraction,
            seed=RANDOM_SEED,
        )
        .repartition(1)
        .limit(max_rows)
    )
    return sampled.toPandas(), safe_fraction


def profile_gold(config):
    table = config["table"]
    displayHTML(f"<h2>{config['title']}</h2><p><code>{table}</code></p>")

    dataframe = spark.table(table)
    display(spark.sql(f"DESCRIBE TABLE EXTENDED {table}"))

    coverage = dataframe.agg(
        *coverage_expressions(dataframe, config["range_columns"])
    )
    coverage_row = coverage.first()
    display(spark.createDataFrame([coverage_row], coverage.schema))

    pandas_sample, effective_fraction = pandas_dataframe(
        dataframe,
        coverage_row["total_rows"],
        config["sample"],
    )
    if pandas_sample.empty:
        raise ValueError(
            f"A amostra de {table} ficou vazia. "
            "Aumente sample_fraction."
        )

    print(
        f"Tabela: {table} | "
        f"Registros no Pandas: {len(pandas_sample):,} | "
        f"Colunas: {len(pandas_sample.columns)} | "
        f"Fração efetiva: {effective_fraction:.8f}"
    )
    display(pandas_sample.head(20))

    profile = ProfileReport(
        pandas_sample,
        title=f"Profiling — {config['title']}",
        explorative=True,
        minimal=False,
    )
    displayHTML(profile.to_html())


for gold_config in GOLD_TABLES:
    profile_gold(gold_config)
