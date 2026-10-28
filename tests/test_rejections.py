from ecommerce_pipeline.rejections import audit_orders
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


def test_sample_orders_reconcile(spark):
    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("order_date", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("status", StringType(), True),
    ])
    orders = spark.createDataFrame([
        (1, 1, 10, "2026-01-01", 1, "delivered"),
        (1, 1, 10, "2026-01-01", 1, "delivered"),
        (2, 999, 10, "2026-01-01", 1, "delivered"),
        (3, 1, 10, "2026-01-01", -1, "delivered"),
    ], schema)
    customers = spark.createDataFrame([(1,)], ["customer_id"])
    products = spark.createDataFrame([(10,)], ["product_id"])

    accepted, rejected = audit_orders(orders, customers, products)
    assert accepted.count() == 1
    assert sum(r["rejected_count"] for r in rejected.collect()) == 3
