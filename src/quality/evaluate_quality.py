"""Avalia regras governadas de qualidade nas camadas Yellow da NYC TLC."""

from __future__ import annotations

import argparse
import calendar
from datetime import date, datetime, timezone
from functools import reduce
from uuid import uuid4
from zoneinfo import ZoneInfo

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


CANONICAL_COLUMNS = {
    "vendor_id": "long",
    "passenger_count": "double",
    "total_amount": "double",
    "pickup_datetime": "timestamp",
    "dropoff_datetime": "timestamp",
    "reference_month": "string",
}
SUPPORTED_LAYERS = ("landing", "bronze", "silver", "gold")

RESULT_SCHEMA = StructType(
    [
        StructField("execution_id", StringType(), False),
        StructField("executed_at", TimestampType(), False),
        StructField("rule_id", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("reference_start", DateType(), True),
        StructField("reference_end", DateType(), True),
        StructField("total_rows", LongType(), False),
        StructField("violation_count", LongType(), False),
        StructField("violation_percentage", DoubleType(), False),
        StructField("maximum_violation_percentage", DoubleType(), False),
        StructField("status", StringType(), False),
        StructField("details", StringType(), True),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--landing-path", required=True)
    parser.add_argument("--bronze-schema", required=True)
    parser.add_argument("--silver-schema", required=True)
    parser.add_argument("--gold-schema", required=True)
    parser.add_argument("--quality-schema", required=True)
    parser.add_argument("--start-month", required=True)
    parser.add_argument("--end-month", required=True)
    parser.add_argument(
        "--layers",
        default=",".join(SUPPORTED_LAYERS),
        help="Camadas separadas por vírgula.",
    )
    parser.add_argument(
        "--gate-on-fail",
        choices=("true", "false"),
        default="false",
        help="Interrompe a execução quando uma regra retorna FAIL.",
    )
    args = parser.parse_args()
    args.layers = tuple(
        layer.strip()
        for layer in args.layers.split(",")
        if layer.strip()
    )
    invalid_layers = set(args.layers) - set(SUPPORTED_LAYERS)
    if not args.layers or invalid_layers:
        parser.error(
            "layers inválidas: "
            f"{', '.join(sorted(invalid_layers)) or 'nenhuma informada'}"
        )
    args.gate_on_fail = args.gate_on_fail == "true"
    return args


def month_sequence(start_month: str, end_month: str) -> list[str]:
    start = datetime.strptime(start_month, "%Y-%m")
    end = datetime.strptime(end_month, "%Y-%m")
    if start > end:
        raise ValueError("start-month deve ser anterior ou igual a end-month")

    months = []
    current = start
    while current <= end:
        months.append(current.strftime("%Y-%m"))
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    return months


def canonical_projection(
    frame: DataFrame,
    vendor_id: str,
    passenger_count: str,
    total_amount: str,
    pickup_datetime: str,
    dropoff_datetime: str,
    reference_month: str | None = None,
) -> DataFrame:
    return frame.select(
        F.col(vendor_id)
        .cast(CANONICAL_COLUMNS["vendor_id"])
        .alias("vendor_id"),
        F.col(passenger_count)
        .cast(CANONICAL_COLUMNS["passenger_count"])
        .alias("passenger_count"),
        F.col(total_amount)
        .cast(CANONICAL_COLUMNS["total_amount"])
        .alias("total_amount"),
        F.col(pickup_datetime)
        .cast(CANONICAL_COLUMNS["pickup_datetime"])
        .alias("pickup_datetime"),
        F.col(dropoff_datetime)
        .cast(CANONICAL_COLUMNS["dropoff_datetime"])
        .alias("dropoff_datetime"),
        (
            F.col(reference_month)
            if reference_month
            else F.lit(None)
        )
        .cast(CANONICAL_COLUMNS["reference_month"])
        .alias("reference_month"),
    )


def landing_frame(
    spark: SparkSession,
    landing_path: str,
    months: list[str],
) -> DataFrame:
    """Lê cada Parquet isoladamente para neutralizar diferenças físicas."""
    frames = [
        canonical_projection(
            spark.read.parquet(
                f"{landing_path}/yellow/{month[:4]}/{month[5:7]}/"
                f"yellow_tripdata_{month}.parquet"
            ),
            "VendorID",
            "passenger_count",
            "total_amount",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
        ).withColumn("reference_month", F.lit(month))
        for month in months
    ]
    return reduce(lambda left, right: left.unionByName(right), frames)


def table_frame(
    spark: SparkSession,
    table_name: str,
    reference_filter,
    vendor_id: str,
    reference_month: str | None = None,
) -> DataFrame:
    frame = spark.read.table(table_name).filter(reference_filter)
    pickup = "tpep_pickup_datetime"
    dropoff = "tpep_dropoff_datetime"
    return canonical_projection(
        frame,
        vendor_id,
        "passenger_count",
        "total_amount",
        pickup,
        dropoff,
        reference_month,
    )


def load_active_rules(
    spark: SparkSession,
    catalog: str,
    quality_schema: str,
) -> list[dict]:
    rows = (
        spark.table(f"{catalog}.{quality_schema}.dq_rule_catalog")
        .filter(F.col("active"))
        .select(
            "rule_id",
            "expression",
            "severity",
            "maximum_violation_percentage",
            "layers",
        )
        .collect()
    )
    if not rows:
        raise RuntimeError("nenhuma regra ativa encontrada no catálogo de DQ")
    return [row.asDict(recursive=True) for row in rows]


def evaluate_layer(
    frame: DataFrame,
    layer: str,
    table_name: str,
    rules: list[dict],
    execution_id: str,
    executed_at: datetime,
    reference_start: date,
    reference_end: date,
) -> list[dict]:
    applicable = [rule for rule in rules if layer in rule["layers"]]
    aggregations = [F.count(F.lit(1)).alias("total_rows")]
    aggregations.extend(
        F.sum(
            F.when(F.coalesce(F.expr(rule["expression"]), F.lit(False)), 1)
            .otherwise(0)
        ).alias(rule["rule_id"])
        for rule in applicable
    )
    metrics = frame.agg(*aggregations).first().asDict()
    total_rows = int(metrics["total_rows"])

    results = []
    for rule in applicable:
        violation_count = int(metrics[rule["rule_id"]] or 0)
        violation_percentage = (
            100.0 * violation_count / total_rows if total_rows else 0.0
        )
        maximum = float(rule["maximum_violation_percentage"])
        status = (
            "OK"
            if violation_percentage <= maximum
            else rule["severity"].upper()
        )
        results.append(
            {
                "execution_id": execution_id,
                "executed_at": executed_at,
                "rule_id": rule["rule_id"],
                "layer": layer,
                "table_name": table_name,
                "reference_start": reference_start,
                "reference_end": reference_end,
                "total_rows": total_rows,
                "violation_count": violation_count,
                "violation_percentage": violation_percentage,
                "maximum_violation_percentage": maximum,
                "status": status,
                "details": (
                    "A condição versionada no catálogo define os registros "
                    "contabilizados como violação."
                ),
            }
        )
    return results


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()
    months = month_sequence(args.start_month, args.end_month)
    execution_id = str(uuid4())
    executed_at = datetime.now(timezone.utc)
    start_year, start_number = map(int, args.start_month.split("-"))
    end_year, end_number = map(int, args.end_month.split("-"))
    reference_start = date(start_year, start_number, 1)
    reference_end = date(
        end_year,
        end_number,
        calendar.monthrange(end_year, end_number)[1],
    )

    bronze_table = (
        f"{args.catalog}.{args.bronze_schema}.bronze_yellow_trips"
    )
    silver_table = (
        f"{args.catalog}.{args.silver_schema}.silver_yellow_trips"
    )
    gold_table = (
        f"{args.catalog}.{args.gold_schema}.gold_yellow_trips_consumption"
    )
    month_filter = F.col("_reference_month").between(
        args.start_month,
        args.end_month,
    )
    layers = []
    if "landing" in args.layers:
        layers.append((
            "landing",
            f"{args.landing_path}/yellow",
            landing_frame(spark, args.landing_path, months),
        ))
    if "bronze" in args.layers:
        layers.append((
            "bronze",
            bronze_table,
            table_frame(
                spark,
                bronze_table,
                month_filter,
                "VendorID",
                "_reference_month",
            ),
        ))
    if "silver" in args.layers:
        layers.append((
            "silver",
            silver_table,
            table_frame(
                spark,
                silver_table,
                month_filter,
                "vendor_id",
                "_reference_month",
            ),
        ))
    if "gold" in args.layers:
        layers.append((
            "gold",
            gold_table,
            table_frame(
                spark,
                gold_table,
                F.lit(True),
                "VendorID",
                "_reference_month",
            ),
        ))
    rules = load_active_rules(spark, args.catalog, args.quality_schema)
    results = []
    for layer, table_name, frame in layers:
        results.extend(
            evaluate_layer(
                frame,
                layer,
                table_name,
                rules,
                execution_id,
                executed_at,
                reference_start,
                reference_end,
            )
        )

    target = (
        f"{args.catalog}.{args.quality_schema}.dq_rule_results"
    )
    (
        spark.createDataFrame(results, RESULT_SCHEMA)
        .write.mode("append")
        .saveAsTable(target)
    )
    execution_date = executed_at.astimezone(
        ZoneInfo("America/Sao_Paulo")
    ).date()
    spark.sql(
        f"""
        DELETE FROM {target}
        WHERE execution_id <> '{execution_id}'
          AND to_date(
            from_utc_timestamp(executed_at, 'America/Sao_Paulo')
          ) = DATE '{execution_date.isoformat()}'
        """
    )
    print(
        f"Execução {execution_id}: {len(results)} resultados gravados em "
        f"{target} para {args.start_month} a {args.end_month}."
    )
    failed = [
        result
        for result in results
        if result["status"] == "FAIL"
    ]
    if args.gate_on_fail and failed:
        failures = ", ".join(
            f"{result['layer']}.{result['rule_id']}"
            for result in failed
        )
        raise RuntimeError(
            "gate de qualidade bloqueou a promoção para Gold: "
            f"{len(failed)} resultado(s) FAIL ({failures})"
        )


if __name__ == "__main__":
    main()
