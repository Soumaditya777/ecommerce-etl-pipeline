from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _require_no_rows(
    df: DataFrame,
    condition,
    message: str,
) -> None:
    """Fail if any records violate a quality rule."""
    if df.filter(condition).limit(1).count() > 0:
        raise ValueError(f"Data quality check failed: {message}")


def _require_unique(
    df: DataFrame,
    key: str,
    dataset_name: str,
) -> None:
    """Ensure a primary key is present and unique."""
    _require_no_rows(
        df,
        F.col(key).isNull(),
        f"{dataset_name}.{key} contains null values",
    )

    duplicate_exists = (
        df.groupBy(key)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
        > 0
    )

    if duplicate_exists:
        raise ValueError(
            f"Data quality check failed: "
            f"{dataset_name}.{key} is not unique"
        )


def validate_silver(
    silver_data: dict[str, DataFrame],
) -> dict[str, int]:
    """Validate Silver datasets and return their row counts."""
    customers = silver_data["customers"]
    products = silver_data["products"]
    orders = silver_data["orders"]

    _require_unique(customers, "customer_id", "customers")
    _require_unique(products, "product_id", "products")
    _require_unique(orders, "order_id", "orders")

    _require_no_rows(
        customers,
        F.col("customer_name").isNull()
        | F.col("city").isNull()
        | F.col("signup_date").isNull(),
        "customers contains missing required values",
    )

    _require_no_rows(
        products,
        F.col("product_name").isNull()
        | F.col("category").isNull()
        | F.col("unit_price").isNull()
        | (F.col("unit_price") < 0),
        "products contains missing values or negative prices",
    )

    _require_no_rows(
        orders,
        F.col("customer_id").isNull()
        | F.col("product_id").isNull()
        | F.col("order_date").isNull()
        | F.col("quantity").isNull()
        | (F.col("quantity") <= 0)
        | F.col("status").isNull(),
        "orders contains invalid required values",
    )

    invalid_customers = orders.join(
        customers.select("customer_id"),
        on="customer_id",
        how="left_anti",
    )

    if invalid_customers.limit(1).count() > 0:
        raise ValueError(
            "Data quality check failed: orders reference "
            "unknown customers"
        )

    invalid_products = orders.join(
        products.select("product_id"),
        on="product_id",
        how="left_anti",
    )

    if invalid_products.limit(1).count() > 0:
        raise ValueError(
            "Data quality check failed: orders reference "
            "unknown products"
        )

    counts = {
        name: df.count()
        for name, df in silver_data.items()
    }

    print("All Silver data-quality checks passed.")
    print("Validated row counts:", counts)

    return counts
