from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config import BRONZE_DIR, SILVER_DIR


def write_bronze(raw_data: dict[str, DataFrame]) -> None:
    """Persist ingested source data as Bronze Parquet."""
    for name, df in raw_data.items():
        (
            df.write
            .mode("overwrite")
            .parquet(str(BRONZE_DIR / name))
        )


def transform_silver(
    raw_data: dict[str, DataFrame],
) -> dict[str, DataFrame]:
    """Clean source data and retain valid order relationships."""

    customers = (
        raw_data["customers"]
        .dropDuplicates(["customer_id"])
        .withColumn(
            "signup_date",
            F.to_date("signup_date", "yyyy-MM-dd")
        )
        .withColumn("city", F.trim(F.col("city")))
    )

    products = (
        raw_data["products"]
        .dropDuplicates(["product_id"])
        .withColumn("product_name", F.trim(F.col("product_name")))
        .withColumn("category", F.trim(F.col("category")))
    )

    orders = (
        raw_data["orders"]
        .dropDuplicates(["order_id"])
        .withColumn(
            "order_date",
            F.to_date("order_date", "yyyy-MM-dd")
        )
        .withColumn("status", F.lower(F.trim(F.col("status"))))
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("product_id").isNotNull())
        .filter(F.col("order_date").isNotNull())
        .filter(F.col("quantity") > 0)
    )

    # Keep only orders that reference existing customers and products.
    valid_customer_orders = orders.join(
        customers.select("customer_id"),
        on="customer_id",
        how="left_semi",
    )

    valid_orders = valid_customer_orders.join(
        products.select("product_id"),
        on="product_id",
        how="left_semi",
    )

    return {
        "customers": customers,
        "products": products,
        "orders": valid_orders,
    }


def write_silver(
    silver_data: dict[str, DataFrame],
) -> None:
    """Persist cleaned datasets as Silver Parquet."""
    for name, df in silver_data.items():
        (
            df.write
            .mode("overwrite")
            .parquet(str(SILVER_DIR / name))
        )
