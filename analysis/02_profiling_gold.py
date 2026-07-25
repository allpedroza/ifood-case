# Databricks notebook source
# MAGIC %md
# MAGIC # Profiling da camada Gold
# MAGIC
# MAGIC Este notebook examina a natureza, distribuição, ausência, cardinalidade
# MAGIC e correlação dos atributos publicados para consumo em
# MAGIC `case_ifood.tlc_data_gold.gold_yellow_trips_consumption`.
# MAGIC
# MAGIC As métricas gerais são calculadas sobre a tabela completa com Spark. O
# MAGIC relatório detalhado converte somente uma amostra controlada para Pandas,
# MAGIC evitando carregar toda a Gold na memória do processo Python.

# COMMAND ----------

# MAGIC %pip install fg-data-profiling==4.19.1

# COMMAND ----------

from data_profiling import ProfileReport
from pyspark.sql import functions as F


GOLD_TABLE = "case_ifood.tlc_data_gold.gold_yellow_trips_consumption"
RANDOM_SEED = 42

dbutils.widgets.text("sample_fraction", "0.01", "Fração da amostra")
dbutils.widgets.text("max_rows", "100000", "Máximo de registros no Pandas")

sample_fraction = float(dbutils.widgets.get("sample_fraction"))
max_rows = int(dbutils.widgets.get("max_rows"))

if not 0 < sample_fraction <= 1:
    raise ValueError("sample_fraction deve estar no intervalo (0, 1].")
if max_rows <= 0:
    raise ValueError("max_rows deve ser maior que zero.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema e metadados da Gold

# COMMAND ----------

gold = spark.table(GOLD_TABLE)

display(
    spark.sql(f"DESCRIBE TABLE EXTENDED {GOLD_TABLE}")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cobertura completa
# MAGIC
# MAGIC Esta célula usa Spark e considera todos os registros, sem amostragem.

# COMMAND ----------

coverage_expressions = [
    F.count(F.lit(1)).alias("total_rows"),
    *[
        F.count(F.when(F.col(column).isNull(), 1)).alias(
            f"{column}__null_count"
        )
        for column in gold.columns
    ],
    F.min("tpep_pickup_datetime").alias("min_pickup_datetime"),
    F.max("tpep_pickup_datetime").alias("max_pickup_datetime"),
]

display(gold.agg(*coverage_expressions))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Amostra convertida para Pandas
# MAGIC
# MAGIC A seleção é reproduzível pela semente fixa. `max_rows` funciona como
# MAGIC proteção adicional de memória mesmo quando uma fração maior é informada.

# COMMAND ----------

sample = (
    gold.sample(
        withReplacement=False,
        fraction=sample_fraction,
        seed=RANDOM_SEED,
    )
    .limit(max_rows)
)

pandas_sample = sample.toPandas()

if pandas_sample.empty:
    raise ValueError(
        "A amostra ficou vazia. Aumente o parâmetro sample_fraction."
    )

print(
    f"Registros enviados ao Pandas: {len(pandas_sample):,} | "
    f"Colunas: {len(pandas_sample.columns)}"
)
display(pandas_sample.head(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Relatório de profiling
# MAGIC
# MAGIC O relatório identifica tipos inferidos, estatísticas descritivas,
# MAGIC valores ausentes, duplicidades, cardinalidade, distribuições, outliers,
# MAGIC alertas e correlações da amostra.

# COMMAND ----------

profile = ProfileReport(
    pandas_sample,
    title="Profiling — Gold Yellow Taxi",
    explorative=True,
    minimal=False,
)

displayHTML(profile.to_html())
