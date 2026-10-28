from datetime import datetime
import time
from pathlib import Path
import json
from .rejections import audit_orders

from pyspark.sql import SparkSession

from .config import SPARK_APP_NAME, SPARK_MASTER, SHUFFLE_PARTITIONS, OUTPUT_DIR
from .ingestion import read_raw_data
from .transformations import (
    write_bronze,
    transform_silver,
    write_silver,
)
from .quality import validate_silver
from .gold import build_gold, write_gold
from .logging_config import setup_logging


def run_pipeline() -> None:
    """Execute the complete e-commerce data pipeline."""
    logger = setup_logging()
    success_path = Path(OUTPUT_DIR) / "_SUCCESS.json"
    success_path.unlink(missing_ok=True)
    spark = None
    start_time = time.perf_counter()

    try:
        logger.info("Starting e-commerce data pipeline")

        spark = (
            SparkSession.builder
            .appName(SPARK_APP_NAME)
            .master(SPARK_MASTER)
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
            .getOrCreate()
        )

        logger.info("Spark session created: %s", spark.version)

        # 1. Ingest raw CSV data
        raw_data = read_raw_data(spark)
        raw_counts = {
            name: df.count()
            for name, df in raw_data.items()
        }
        logger.info("Raw input row counts: %s", raw_counts)

        # 2. Persist Bronze

        # Audit orders before Silver transformations.
        accepted_orders, rejected_orders = audit_orders(
            raw_data["orders"], raw_data["customers"], raw_data["products"]
        )

        output_root = Path(OUTPUT_DIR)
        rejected_path = output_root / "rejected" / "orders"
        rejected_orders.write.mode("overwrite").parquet(str(rejected_path))

        raw_order_count = raw_data["orders"].count()
        accepted_count = accepted_orders.count()
        rejected_count = rejected_orders.agg(
            {"rejected_count": "sum"}
        ).first()[0] or 0

        if accepted_count + rejected_count != raw_order_count:
            raise ValueError(
                f"Order reconciliation failed: raw={raw_order_count}, "
                f"accepted={accepted_count}, rejected={rejected_count}"
            )

        logger.info(
            "Order audit passed: raw=%d accepted=%d rejected=%d",
            raw_order_count, accepted_count, rejected_count
        )
        audited_raw_data = dict(raw_data)
        audited_raw_data["orders"] = accepted_orders

        write_bronze(raw_data)
        logger.info("Bronze datasets written")

        # 3. Transform and persist Silver
        silver_data = transform_silver(audited_raw_data)
        quality_counts = validate_silver(silver_data)
        write_silver(silver_data)
        logger.info("Silver datasets written")

        # 4. Build and persist Gold
        gold_data = build_gold(silver_data)
        write_gold(gold_data)

        success_path = output_root / "_SUCCESS.json"
        success_path.write_text(
            json.dumps({
                "status": "success",
                "raw_orders": raw_order_count,
                "accepted_orders": accepted_count,
                "rejected_orders": int(rejected_count),
                "completed_at": datetime.now().isoformat(),
            }, indent=2) + "\n",
            encoding="utf-8",
        )
        logger.info("Gold datasets written: %s", list(gold_data))

        # 5. Report completion metrics
        total_revenue = (
            gold_data["orders_enriched"]
            .agg({"revenue": "sum"})
            .first()[0]
        )

        elapsed = time.perf_counter() - start_time

        logger.info("Validated Silver row counts: %s", quality_counts)
        logger.info("Total order revenue: %s", total_revenue)
        logger.info("Pipeline completed in %.2f seconds", elapsed)

    except Exception:
        logger.exception("Pipeline failed")
        raise

    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    run_pipeline()
