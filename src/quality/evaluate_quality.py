"""Avalia regras governadas de qualidade nas camadas Yellow da NYC TLC."""

from __future__ import annotations

import argparse
import calendar
from datetime import date, datetime, timezone
from functools import reduce
from uuid import uuid4

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
    "passenger_count": "double",
    "total_amount": "double",
    "pickup_datetime": "timestamp",
    "dropoff_datetime": "timestamp",
}

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
    return parser.parse_args()


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
    passenger_count: str,
    total_amount: str,
    pickup_datetime: str,
    dropoff_datetime: str,
) -> DataFrame:
    return frame.select(
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
            "passenger_count",
            "total_amount",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
        )
        for month in months
    ]
    return reduce(lambda left, right: left.unionByName(right), frames)


def table_frame(
    spark: SparkSession,
    table_name: str,
    reference_filter,
) -> DataFrame:
    frame = spark.read.table(table_name).filter(reference_filter)
    pickup = "tpep_pickup_datetime"
    dropoff = "tpep_dropoff_datetime"
    return canonical_projection(
        frame,
        "passenger_count",
        "total_amount",
        pickup,
        dropoff,
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
                    "Registros nulos não são violações desta regra; "
                    "serão tratados separadamente como completude."
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
    layers = [
        (
            "landing",
            f"{args.landing_path}/yellow",
            landing_frame(spark, args.landing_path, months),
        ),
        (
            "bronze",
            bronze_table,
            table_frame(
                spark,
                bronze_table,
                month_filter,
            ),
        ),
        (
            "silver",
            silver_table,
            table_frame(
                spark,
                silver_table,
                month_filter,
            ),
        ),
        (
            "gold",
            gold_table,
            table_frame(
                spark,
                gold_table,
                F.lit(True),
            ),
        ),
    ]
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
    print(
        f"Execução {execution_id}: {len(results)} resultados gravados em "
        f"{target} para {args.start_month} a {args.end_month}."
    )


if __name__ == "__main__":
    main()
