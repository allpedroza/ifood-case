"""Materializa o contrato versionado de qualidade no Unity Catalog."""

from __future__ import annotations

import argparse
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)


RULE_CATALOG_SCHEMA = StructType(
    [
        StructField("rule_id", StringType(), False),
        StructField("rule_name", StringType(), False),
        StructField("dimension", StringType(), False),
        StructField("description", StringType(), False),
        StructField("expression", StringType(), False),
        StructField("severity", StringType(), False),
        StructField("maximum_violation_percentage", DoubleType(), False),
        StructField("layers", ArrayType(StringType(), containsNull=False), False),
        StructField("active", BooleanType(), False),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--quality-schema", required=True)
    parser.add_argument("--rules-file", required=True)
    return parser.parse_args()


def load_rules(path: str) -> list[dict]:
    with Path(path).open(encoding="utf-8") as rules_file:
        contract = yaml.safe_load(rules_file)

    defaults = contract.get("defaults", {})
    rows = []
    for rule in contract["rules"]:
        rows.append(
            {
                "rule_id": rule["id"],
                "rule_name": rule["name"],
                "dimension": rule["dimension"],
                "description": rule["description"],
                "expression": rule["expression"],
                "severity": rule.get("severity", defaults["severity"]),
                "maximum_violation_percentage": float(
                    rule.get(
                        "maximum_violation_percentage",
                        defaults["maximum_violation_percentage"],
                    )
                ),
                "layers": rule["layers"],
                "active": bool(rule.get("active", defaults["active"])),
            }
        )
    return rows


def create_quality_objects(
    spark: SparkSession,
    catalog: str,
    quality_schema: str,
) -> None:
    namespace = f"`{catalog}`.`{quality_schema}`"

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {namespace}.dq_rule_catalog (
          rule_id STRING NOT NULL COMMENT 'Identificador estável da regra',
          rule_name STRING NOT NULL COMMENT 'Nome legível da regra',
          dimension STRING NOT NULL COMMENT 'Dimensão de qualidade avaliada',
          description STRING NOT NULL COMMENT 'Descrição funcional da regra',
          expression STRING NOT NULL COMMENT 'Expressão lógica versionada no contrato',
          severity STRING NOT NULL COMMENT 'Severidade da violação',
          maximum_violation_percentage DOUBLE NOT NULL
            COMMENT 'Percentual máximo de violações aceito',
          layers ARRAY<STRING> NOT NULL COMMENT 'Camadas em que a regra se aplica',
          active BOOLEAN NOT NULL COMMENT 'Indica se a regra deve ser executada',
          contract_version INT NOT NULL COMMENT 'Versão do contrato de regras',
          created_at TIMESTAMP NOT NULL COMMENT 'Data de criação no catálogo',
          updated_at TIMESTAMP NOT NULL COMMENT 'Data da última sincronização'
        )
        USING DELTA
        COMMENT 'Catálogo governado e versionado de regras de qualidade de dados'
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {namespace}.dq_rule_results (
          execution_id STRING NOT NULL COMMENT 'Identificador da execução de qualidade',
          executed_at TIMESTAMP NOT NULL COMMENT 'Data e hora da avaliação',
          rule_id STRING NOT NULL COMMENT 'Regra avaliada',
          layer STRING NOT NULL COMMENT 'Camada avaliada',
          table_name STRING NOT NULL COMMENT 'Tabela ou conjunto de arquivos avaliado',
          reference_start DATE COMMENT 'Início da janela de referência',
          reference_end DATE COMMENT 'Fim da janela de referência',
          total_rows BIGINT NOT NULL COMMENT 'Quantidade total de registros avaliados',
          violation_count BIGINT NOT NULL COMMENT 'Quantidade de registros que violaram a regra',
          violation_percentage DOUBLE NOT NULL COMMENT 'Percentual de registros que violaram a regra',
          maximum_violation_percentage DOUBLE NOT NULL
            COMMENT 'Percentual máximo de violações aceito',
          status STRING NOT NULL COMMENT 'Resultado OK, WARN ou FAIL',
          details STRING COMMENT 'Detalhes adicionais da avaliação'
        )
        USING DELTA
        COMMENT 'Histórico de resultados das regras de qualidade por camada'
        """
    )

    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {namespace}.dq_execution_summary
        COMMENT 'Resultado mais recente de cada regra por camada e tabela'
        AS
        SELECT * EXCEPT (result_order)
        FROM (
          SELECT
            results.*,
            ROW_NUMBER() OVER (
              PARTITION BY rule_id, layer, table_name
              ORDER BY executed_at DESC, execution_id DESC
            ) AS result_order
          FROM {namespace}.dq_rule_results AS results
        )
        WHERE result_order = 1
        """
    )

    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {namespace}.dq_dashboard_results
        COMMENT 'Histórico diário enriquecido para o dashboard de qualidade'
        AS
        SELECT
          results.execution_id,
          results.executed_at,
          to_date(
            from_utc_timestamp(results.executed_at, 'America/Sao_Paulo')
          ) AS execution_date,
          results.rule_id,
          rules.rule_name,
          rules.dimension,
          rules.description,
          rules.severity,
          results.layer,
          results.table_name,
          results.reference_start,
          results.reference_end,
          results.total_rows,
          results.violation_count,
          results.violation_percentage,
          results.maximum_violation_percentage,
          results.status,
          results.details
        FROM {namespace}.dq_rule_results AS results
        INNER JOIN {namespace}.dq_rule_catalog AS rules
          ON results.rule_id = rules.rule_id
        """
    )

    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {namespace}.dq_dashboard_table_success
        COMMENT 'Percentual diário de verificações aprovadas por tabela'
        AS
        SELECT
          execution_id,
          executed_at,
          execution_date,
          layer,
          table_name,
          reference_start,
          reference_end,
          COUNT(*) AS evaluated_rules,
          SUM(total_rows) AS evaluated_checks,
          SUM(violation_count) AS failed_checks,
          ROUND(
            100.0 * (
              1.0 - SUM(violation_count) / NULLIF(SUM(total_rows), 0)
            ),
            6
          ) AS success_percentage
        FROM {namespace}.dq_dashboard_results
        GROUP BY ALL
        """
    )

    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {namespace}.dq_dashboard_dimension_success
        COMMENT 'Percentual diário de verificações aprovadas por dimensão'
        AS
        SELECT
          execution_id,
          executed_at,
          execution_date,
          layer,
          table_name,
          dimension,
          reference_start,
          reference_end,
          COUNT(*) AS evaluated_rules,
          SUM(total_rows) AS evaluated_checks,
          SUM(violation_count) AS failed_checks,
          ROUND(
            100.0 * (
              1.0 - SUM(violation_count) / NULLIF(SUM(total_rows), 0)
            ),
            6
          ) AS success_percentage
        FROM {namespace}.dq_dashboard_results
        GROUP BY ALL
        """
    )

    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {namespace}.dq_dashboard_run_summary
        COMMENT 'Resumo diário das execuções para indicadores do dashboard'
        AS
        SELECT
          execution_id,
          executed_at,
          execution_date,
          reference_start,
          reference_end,
          COUNT(*) AS evaluated_rules,
          COUNT_IF(status = 'OK') AS ok_results,
          COUNT_IF(status = 'WARN') AS warn_results,
          COUNT_IF(status = 'FAIL') AS fail_results,
          COUNT(DISTINCT layer) AS evaluated_layers
        FROM {namespace}.dq_dashboard_results
        GROUP BY ALL
        """
    )


def synchronize_catalog(
    spark: SparkSession,
    catalog: str,
    quality_schema: str,
    rules: list[dict],
    contract_version: int,
) -> None:
    target = f"`{catalog}`.`{quality_schema}`.dq_rule_catalog"
    source = (
        spark.createDataFrame(rules, RULE_CATALOG_SCHEMA)
        .withColumn("contract_version", F.lit(contract_version))
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )
    source.createOrReplaceTempView("dq_rule_catalog_source")

    spark.sql(
        f"""
        MERGE INTO {target} AS target
        USING dq_rule_catalog_source AS source
          ON target.rule_id = source.rule_id
        WHEN MATCHED THEN UPDATE SET
          target.rule_name = source.rule_name,
          target.dimension = source.dimension,
          target.description = source.description,
          target.expression = source.expression,
          target.severity = source.severity,
          target.maximum_violation_percentage = source.maximum_violation_percentage,
          target.layers = source.layers,
          target.active = source.active,
          target.contract_version = source.contract_version,
          target.updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT *
        WHEN NOT MATCHED BY SOURCE THEN UPDATE SET
          target.active = false,
          target.updated_at = current_timestamp()
        """
    )


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    with Path(args.rules_file).open(encoding="utf-8") as rules_file:
        contract = yaml.safe_load(rules_file)

    create_quality_objects(spark, args.catalog, args.quality_schema)
    synchronize_catalog(
        spark,
        args.catalog,
        args.quality_schema,
        load_rules(args.rules_file),
        int(contract["version"]),
    )
    print(
        "Contrato de qualidade sincronizado em "
        f"{args.catalog}.{args.quality_schema}."
    )


if __name__ == "__main__":
    main()
