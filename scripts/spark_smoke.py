"""Exercise the first funnel with Spark SQL in local mode."""

from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession


def main():
    source = Path(__file__).resolve().parents[1] / "tests/fixtures/events.jsonl"
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("EcommerceWarehouseSmoke")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        schema = (
            "event_id string, user_id string, session_id string, "
            "event_time timestamp, event_name string, amount_usd string"
        )
        raw = spark.read.schema(schema).option("mode", "FAILFAST").json(source.as_uri())
        raw.selectExpr(
            "event_id", "user_id", "session_id", "event_time", "event_name",
            "CAST(amount_usd AS DECIMAL(18, 2)) AS amount_usd",
        ).distinct().createOrReplaceTempView("events")

        result = spark.sql("""
            WITH ordered AS (
                SELECT *,
                       MIN(CASE WHEN event_name = 'view_item' THEN event_time END)
                           OVER (PARTITION BY user_id, session_id) AS viewed_at
                FROM events
                WHERE user_id IS NOT NULL AND session_id IS NOT NULL
            ),
            cart AS (
                SELECT *,
                       MIN(CASE WHEN event_name = 'add_to_cart' AND event_time > viewed_at
                                THEN event_time END)
                           OVER (PARTITION BY user_id, session_id) AS added_at
                FROM ordered
            ),
            sessions AS (
                SELECT user_id, session_id, TO_DATE(MIN(event_time)) AS start_date,
                       MAX(viewed_at) AS viewed_at,
                       MAX(added_at) AS added_at,
                       MIN(CASE WHEN event_name = 'purchase' AND event_time > added_at
                                THEN event_time END) AS purchased_at
                FROM cart
                GROUP BY user_id, session_id
            )
            SELECT CAST(start_date AS STRING) AS start_date,
                   COUNT(*) AS sessions, COUNT(viewed_at) AS views,
                   COUNT(added_at) AS carts, COUNT(purchased_at) AS purchases
            FROM sessions
            GROUP BY start_date
        """).collect()
        purchases = spark.sql("""
            SELECT COUNT(*), SUM(amount_usd), COUNT(*) - COUNT(amount_usd)
            FROM events
            WHERE event_name = 'purchase'
        """).first()
        expected = ([("2020-11-10", 3, 3, 1, 1)], (3, Decimal("50.00"), 1))
        actual = ([tuple(row) for row in result], tuple(purchases))
        if actual != expected:
            raise AssertionError(f"Expected {expected}, got {actual}")
        print(f"Spark {spark.version} PASS: sessions and funnel={result}, purchases={purchases}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
