from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_DIR / "data" / "raw"
OUTPUT_DIR = PROJECT_DIR / "output"
LOG_DIR = PROJECT_DIR / "logs"

BRONZE_DIR = OUTPUT_DIR / "bronze"
SILVER_DIR = OUTPUT_DIR / "silver"
GOLD_DIR = OUTPUT_DIR / "gold"

SPARK_APP_NAME = "EcommerceDataEngineering"
SPARK_MASTER = "local[*]"
SHUFFLE_PARTITIONS = 4
