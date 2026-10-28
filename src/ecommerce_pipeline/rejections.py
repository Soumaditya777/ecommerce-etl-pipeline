from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def audit_orders(
    raw_orders: DataFrame,
    customers: DataFrame,
    products: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Return (accepted_orders, rejected_orders).

    Rejected report columns include all original order fields plus:
      rejection_reason: explanation for rejection
      rejected_count: number of physical input rows represented

    Exact duplicates retain one copy and report only the extra copies.
    Conflicting order IDs reject every version of that ID.
    """
    columns = raw_orders.columns

    # Detect IDs that refer to multiple distinct order records.
    distinct_rows = raw_orders.dropDuplicates(columns)
    conflicting_ids = (
        distinct_rows
        .filter(F.col("order_id").isNotNull())
        .groupBy("order_id")
        .count()
        .filter(F.col("count") > 1)
        .select("order_id")
    )

    # Report every physical row associated with a conflicting ID.
    conflicting_report = (
        raw_orders
        .join(conflicting_ids, on="order_id", how="inner")
        .groupBy(*columns)
        .count()
        .withColumn("rejection_reason", F.lit("conflicting_order_id"))
        .withColumn("rejected_count", F.col("count"))
        .drop("count")
        .select(*columns, "rejection_reason", "rejected_count")
    )

    # Exact duplicate rows: only copies beyond the first are rejected.
    non_conflicting = raw_orders.join(
        conflicting_ids, on="order_id", how="left_anti"
    )
    duplicate_report = (
        non_conflicting
        .groupBy(*columns)
        .count()
        .filter(F.col("count") > 1)
        .withColumn("rejection_reason", F.lit("exact_duplicate"))
        .withColumn("rejected_count", F.col("count") - 1)
        .drop("count")
        .select(*columns, "rejection_reason", "rejected_count")
    )

    # Validate distinct, non-conflicting orders.
    candidates = distinct_rows.join(
        conflicting_ids, on="order_id", how="left_anti"
    )

    customer_ids = (
        customers.select("customer_id").distinct()
        .withColumn("_customer_exists", F.lit(True))
    )
    product_ids = (
        products.select("product_id").distinct()
        .withColumn("_product_exists", F.lit(True))
    )

    checked = (
        candidates
        .join(customer_ids, on="customer_id", how="left")
        .join(product_ids, on="product_id", how="left")
    )

    # try_cast turns malformed dates/quantities into nulls instead of
    # crashing the audit. Compatible with Spark SQL versions supporting try_cast.
    parsed_date = F.expr("try_cast(order_date AS DATE)")
    parsed_quantity = F.expr("try_cast(quantity AS INT)")
    normalized_status = F.lower(F.trim(F.col("status")))

    reasons = F.concat_ws(
        "|",
        F.when(F.col("order_id").isNull(), "missing_order_id"),
        F.when(F.col("customer_id").isNull(), "missing_customer_id"),
        F.when(F.col("_customer_exists").isNull(), "unknown_customer"),
        F.when(F.col("product_id").isNull(), "missing_product_id"),
        F.when(F.col("_product_exists").isNull(), "unknown_product"),
        F.when(parsed_date.isNull(), "invalid_order_date"),
        F.when(parsed_quantity.isNull(), "invalid_quantity"),
        F.when(parsed_quantity <= 0, "non_positive_quantity"),
        F.when(
            normalized_status.isNull() | (normalized_status == ""),
            "invalid_status",
        ),
    )

    checked = checked.withColumn("rejection_reason", reasons)

    invalid_report = (
        checked
        .filter(F.col("rejection_reason") != "")
        .drop("_customer_exists", "_product_exists")
        .withColumn("rejected_count", F.lit(1))
        .select(*columns, "rejection_reason", "rejected_count")
    )

    accepted = (
        checked
        .filter(F.col("rejection_reason") == "")
        .drop("_customer_exists", "_product_exists", "rejection_reason")
        .withColumn("order_date", parsed_date.cast("string"))
        .withColumn("quantity", parsed_quantity.cast("int"))
        .withColumn("status", normalized_status)
        .dropDuplicates(columns)
    )

    rejected = (
        invalid_report
        .unionByName(conflicting_report)
        .unionByName(duplicate_report)
    )

    return accepted, rejected
