"""Build the retail category performance warehouse from three source extracts."""

import argparse
import csv
import json
from pathlib import Path
from uuid import uuid4

import pyarrow.dataset as ds
import pyarrow.parquet as pq
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


def require_empty(frame, message):
    if frame.take(1):
        raise ValueError(message)


def require_unique(frame, column):
    require_empty(
        frame.groupBy(column).count().where(F.col(column).isNull() | (F.col("count") > 1)),
        f"Expected unique non-null key: {column}",
    )


def save(frame, batch, name, partition=None):
    # Local portfolio runner. Spark performs the transformations; Arrow writes
    # the result without Hadoop's Windows-specific native write dependency.
    table = frame.toArrow()
    options = {}
    if partition:
        options = {"partitioning": [partition], "partitioning_flavor": "hive"}
    destination = batch / name
    if table.num_rows == 0:
        destination.mkdir(parents=True)
        pq.write_table(table, destination / "part-0.parquet")
    else:
        ds.write_dataset(table, destination, format="parquet", **options)


def build(spark, source, batch):
    reader = spark.read.option("header", True).option("mode", "FAILFAST")
    sales = reader.csv((source / "source_crm/sales_details.csv").as_posix())
    products = reader.csv((source / "source_crm/prd_info.csv").as_posix())
    categories = reader.csv((source / "source_erp/PX_CAT_G1V2.csv").as_posix())

    for name, frame in (("sales", sales), ("products", products), ("categories", categories)):
        save(frame, batch, f"bronze/{name}")

    lines = sales.selectExpr(
        "nullif(trim(sls_ord_num), '') AS order_number",
        "nullif(trim(sls_prd_key), '') AS product_number",
        "CAST(try_to_timestamp(sls_order_dt, 'yyyyMMdd') AS DATE) AS order_date",
        "try_cast(sls_sales AS DECIMAL(18,2)) AS original_amount",
        "try_cast(sls_quantity AS BIGINT) AS units",
        "try_cast(sls_price AS DECIMAL(18,2)) AS unit_price",
    ).withColumn(
        "sales_amount",
        F.expr("CAST(coalesce(units * abs(unit_price), original_amount) AS DECIMAL(24,2))"),
    ).withColumn(
        "rejection",
        F.expr("""CASE
            WHEN order_number IS NULL THEN 'missing_order'
            WHEN order_date IS NULL THEN 'invalid_order_date'
            WHEN units IS NULL OR units <= 0 THEN 'invalid_quantity'
            WHEN sales_amount IS NULL OR sales_amount <= 0 THEN 'invalid_amount'
        END"""),
    ).cache()
    rejected = lines.where("rejection IS NOT NULL")
    accepted = lines.where("rejection IS NULL").drop("rejection")
    source_count = lines.count()
    accepted_count = accepted.count()
    rejected_count = rejected.count()
    if not accepted_count or source_count != accepted_count + rejected_count:
        raise ValueError("Sales row reconciliation failed or no valid sales lines found")

    history = products.selectExpr(
        "nullif(trim(substring(prd_key, 7)), '') AS product_number",
        "replace(substring(prd_key, 1, 5), '-', '_') AS category_id",
        "prd_nm AS product_name",
        "try_cast(prd_start_dt AS DATE) AS start_date",
    )
    require_empty(history.where("product_number IS NULL OR start_date IS NULL"), "Invalid product key or version date")
    latest = Window.partitionBy("product_number").orderBy(F.col("start_date").desc())
    current = history.withColumn("version", F.dense_rank().over(latest)).where("version = 1").drop("version")
    dictionary = categories.selectExpr("nullif(trim(ID), '') AS category_id", "CAT AS category", "SUBCAT AS subcategory")
    require_unique(current, "product_number")
    require_unique(dictionary, "category_id")
    dimension = current.join(F.broadcast(dictionary), "category_id", "left")

    save(accepted, batch, "silver/sales")
    save(rejected, batch, "silver/rejected_sales")
    save(dimension, batch, "gold/dim_product")
    fact = accepted.join(F.broadcast(dimension), "product_number", "left").withColumn(
        "sales_month", F.trunc("order_date", "month")
    ).withColumn("category", F.coalesce("category", F.lit("Unknown"))).cache()
    before = accepted.agg(F.count("*"), F.sum("sales_amount")).first()
    after = fact.agg(F.count("*"), F.sum("sales_amount")).first()
    if tuple(before) != tuple(after):
        raise ValueError("The product join changed the sales line count or amount")
    save(fact, batch, "gold/fact_sales", "sales_month")

    monthly = fact.groupBy("sales_month", "category").agg(
        F.sum("sales_amount").alias("sales_amount"),
        F.sum("units").alias("units"),
        F.count("*").alias("sales_lines"),
        F.countDistinct("order_number").alias("orders_in_category"),
    ).cache()
    month_check = monthly.agg(F.sum("sales_lines"), F.sum("sales_amount")).first()
    if tuple(month_check) != tuple(after):
        raise ValueError("Monthly aggregates do not reconcile with the sales facts")
    previous = monthly.select(
        F.add_months("sales_month", 1).alias("sales_month"),
        "category",
        F.col("sales_amount").alias("prior_month_amount"),
    )
    mart = monthly.join(previous, ["sales_month", "category"], "left").withColumn(
        "month_over_month_pct",
        F.round(F.expr("100 * try_divide(sales_amount - prior_month_amount, prior_month_amount)"), 2),
    )
    save(mart, batch, "mart/category_monthly")
    report = batch / "report"
    report.mkdir()
    ordered = mart.orderBy("sales_month", "category")
    with (report / "category_monthly.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(ordered.columns)
        writer.writerows(ordered.toLocalIterator())
    fact_path = batch / "gold/fact_sales"
    files = sorted(file.as_posix() for file in fact_path.rglob("*.parquet"))
    stored_fact = spark.read.option("basePath", fact_path.as_posix()).parquet(*files)
    if "sales_month" not in stored_fact.columns:
        raise ValueError("Spark could not discover the sales month partitions")
    stored_totals = stored_fact.agg(F.count("*"), F.sum("sales_amount")).first()
    if tuple(stored_totals) != tuple(after):
        raise ValueError("Persisted Parquet does not reconcile with the sales facts")
    date_example = fact.agg(F.max("sales_month")).first()[0]
    example_query = stored_fact.where(F.col("sales_month") == F.lit(date_example))
    plan = example_query._jdf.queryExecution().executedPlan().toString()
    (report / "partition_pruning.txt").write_text(plan, encoding="utf-8")
    return {
        "source_sales_lines": source_count,
        "accepted_sales_lines": accepted_count,
        "rejected_sales_lines": rejected_count,
        "fact_sales_amount": str(after[1]),
        "unknown_category_lines": fact.where("category = 'Unknown'").count(),
        "adjusted_sales_lines": accepted.where("NOT (sales_amount <=> original_amount)").count(),
        "first_order_date": str(fact.agg(F.min("order_date")).first()[0]),
        "last_order_date": str(fact.agg(F.max("order_date")).first()[0]),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    root = args.output.resolve()
    batch = root / "runs" / uuid4().hex
    batch.mkdir(parents=True)
    spark = (
        SparkSession.builder.master("local[2]").appName("RetailCategoryWarehouse")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        status = build(spark, args.source.resolve(), batch)
        manifest = json.dumps({"run": batch.name, **status}, indent=2)
        (batch / "audit.json").write_text(manifest, encoding="utf-8")
        pointer = root / f".current-{batch.name}.json"
        pointer.write_text(manifest, encoding="utf-8")
        pointer.replace(root / "current.json")
        print(manifest, flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
