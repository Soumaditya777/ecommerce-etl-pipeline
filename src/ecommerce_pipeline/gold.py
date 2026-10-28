from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .config import GOLD_DIR


def build_gold(
    silver_data: dict[str, DataFrame],
) -> dict[str, DataFrame]:
    """Build analytics-ready Gold datasets from Silver data."""

    customers = silver_data["customers"]
    products = silver_data["products"]
    orders = silver_data["orders"]

    # Add customer and product details to each order.
    orders_enriched = (
        orders
        .join(customers, on="customer_id", how="inner")
        .join(products, on="product_id", how="inner")
        .withColumn(
            "revenue",
            F.col("quantity") * F.col("unit_price"),
        )
        .withColumn(
            "order_month",
            F.date_format("order_date", "yyyy-MM"),
        )
    )

    product_revenue = (
        orders_enriched
        .groupBy("product_id", "product_name", "category")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.sum("revenue").alias("total_revenue"),
            F.countDistinct("order_id").alias("order_count"),
        )
    )

    city_revenue = (
        orders_enriched
        .groupBy("city")
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.countDistinct("order_id").alias("order_count"),
        )
    )

    customer_spending = (
        orders_enriched
        .groupBy("customer_id", "customer_name", "city")
        .agg(
            F.sum("revenue").alias("total_spending"),
            F.countDistinct("order_id").alias("order_count"),
        )
    )

    daily_revenue = (
        orders_enriched
        .groupBy("order_date")
        .agg(
            F.sum("revenue").alias("daily_revenue"),
            F.countDistinct("order_id").alias("order_count"),
        )
    )

    product_rank_window = Window.orderBy(
        F.col("total_revenue").desc()
    )

    ranked_products = product_revenue.withColumn(
        "revenue_rank",
        F.dense_rank().over(product_rank_window),
    )

    running_window = (
        Window.orderBy("order_date")
        .rowsBetween(
            Window.unboundedPreceding,
            Window.currentRow,
        )
    )

    daily_running_total = daily_revenue.withColumn(
        "running_revenue",
        F.sum("daily_revenue").over(running_window),
    )

    # Retain order_month so Spark can partition the output by month.
    orders_partitioned = orders_enriched

    return {
        "orders_enriched": orders_enriched,
        "product_revenue": product_revenue,
        "city_revenue": city_revenue,
        "customer_spending": customer_spending,
        "daily_revenue": daily_revenue,
        "ranked_products": ranked_products,
        "daily_running_total": daily_running_total,
        "orders_partitioned": orders_partitioned,
    }


def write_gold(
    gold_data: dict[str, DataFrame],
) -> None:
    """Write Gold datasets to Parquet."""
    for name, df in gold_data.items():
        writer = df.write.mode("overwrite")

        if name == "orders_partitioned":
            writer.partitionBy("order_month").parquet(
                str(GOLD_DIR / name)
            )
        else:
            writer.parquet(str(GOLD_DIR / name))
