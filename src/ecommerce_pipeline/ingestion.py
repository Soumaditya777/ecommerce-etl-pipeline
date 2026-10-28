from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
)

from .config import DATA_DIR


CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("signup_date", StringType(), True),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("unit_price", DoubleType(), True),
])

ORDERS_SCHEMA = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("status", StringType(), True),
])


def read_raw_data(spark: SparkSession) -> dict[str, DataFrame]:
    """Read raw e-commerce CSV files using explicit schemas."""
    file_schemas = {
        "customers": CUSTOMERS_SCHEMA,
        "products": PRODUCTS_SCHEMA,
        "orders": ORDERS_SCHEMA,
    }

    dataframes = {}

    for name, schema in file_schemas.items():
        file_path = DATA_DIR / f"{name}.csv"

        if not file_path.exists():
            raise FileNotFoundError(
                f"Required input file was not found: {file_path}"
            )

        dataframes[name] = (
            spark.read
            .option("header", True)
            .schema(schema)
            .csv(str(file_path))
        )

    return dataframes
