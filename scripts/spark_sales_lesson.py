"""Compare product join correctness and physical plans using the tutorial CSVs."""

import argparse
from pathlib import Path

from pyspark.sql import SparkSession


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True, help="source_crm directory")
    args = parser.parse_args()
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("SalesJoinLesson")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        sales_path = (args.source / "sales_details.csv").resolve().as_posix()
        product_path = (args.source / "prd_info.csv").resolve().as_posix()
        csv = spark.read.option("header", True).option("mode", "FAILFAST")
        csv.csv(sales_path).selectExpr(
            "sls_ord_num AS order_number",
            "sls_prd_key AS product_number",
            "CAST(sls_sales AS DECIMAL(18,2)) AS sales_amount",
        ).createOrReplaceTempView("sales")
        csv.csv(product_path).selectExpr(
            "substring(prd_key, 7) AS product_number",
            "prd_nm AS product_name",
            "CAST(prd_start_dt AS DATE) AS start_date",
        ).createOrReplaceTempView("product_history")
        spark.sql("""
            SELECT product_number, product_name
            FROM (
                SELECT *, DENSE_RANK() OVER (
                    PARTITION BY product_number ORDER BY start_date DESC
                ) AS version
                FROM product_history
            )
            WHERE version = 1
        """).createOrReplaceTempView("current_products")
        ties = spark.sql("""
            SELECT product_number FROM current_products
            GROUP BY product_number HAVING COUNT(*) > 1
        """).count()
        if ties:
            raise AssertionError(f"{ties} product keys have multiple latest versions")

        def join(table, hint=""):
            return spark.sql(f"""
                SELECT {hint} s.order_number, s.sales_amount, p.product_number
                FROM sales s LEFT JOIN {table} p
                ON s.product_number = p.product_number
            """)

        def totals(frame):
            return frame.selectExpr(
                "COUNT(*) AS rows", "SUM(sales_amount) AS amount"
            ).first()

        original = totals(spark.table("sales"))
        historical = totals(join("product_history"))
        current = join("current_products")
        broadcast = join("current_products", "/*+ BROADCAST(p) */")
        current_totals = totals(current)
        broadcast_totals = totals(broadcast)
        if original != current_totals or current_totals != broadcast_totals:
            raise AssertionError("Sales totals changed after joining unique products")
        unmatched = current.where("product_number IS NULL").count()
        summary = (
            f"Input: {original}\n"
            f"Join with historical products: {historical}\n"
            f"Join with current products: {current_totals}; unmatched={unmatched}\n"
            f"Broadcast join: {broadcast_totals}"
        )
        print(summary, flush=True)
        print("\n--- Sort-merge comparison ---", flush=True)
        current.explain("formatted")
        print("\n--- Explicit broadcast ---", flush=True)
        broadcast.explain("formatted")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
