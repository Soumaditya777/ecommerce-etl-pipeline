# E-Commerce ETL Pipeline

A batch data engineering pipeline built with **Python and Apache PySpark** to ingest, validate, transform, and analyze e-commerce data using a Bronze–Silver–Gold architecture.

The project demonstrates ETL processing, data quality validation, rejection auditing, Parquet storage, analytical aggregations, and automated testing.

## Architecture

```text
          Raw CSV Data
                |
                v
          Data Ingestion
                |
                v
       Bronze Layer (Raw Data)
                |
                v
      Validation & Transformation
           /           \
          v             v
   Silver Layer     Rejected Records
   (Validated)      (Audit Output)
          |
          v
      Gold Layer
   (Analytics & KPIs)
          |
          v
   Success Manifest
   + Execution Logs
```

## Tech Stack

- **Language:** Python
- **Processing Engine:** Apache Spark / PySpark
- **Storage Formats:** CSV, Parquet, JSON
- **Testing:** pytest
- **Data Engineering:** ETL, data validation, rejection auditing, analytical transformations

## Key Features

- **ETL processing:** Ingests customer, product, and order data from CSV files.
- **Medallion architecture:** Organizes processing into Bronze, Silver, and Gold layers.
- **Data quality checks:** Validates required fields, references, quantities, dates, and order statuses.
- **Rejection auditing:** Records rejected order rows with reasons, including invalid quantities, unknown customers, and duplicate records.
- **Duplicate handling:** Distinguishes exact duplicate records from conflicting versions of the same order ID.
- **Parquet output:** Stores processed datasets in a columnar format suitable for analytical workloads.
- **Revenue analytics:** Generates product revenue, city revenue, customer spending, and daily revenue datasets.
- **Window functions:** Supports product ranking and daily running revenue totals.
- **Data reconciliation:** Verifies that raw order rows reconcile with accepted and rejected records.
- **Execution tracking:** Writes a JSON success manifest with processing counts and completion time.
- **Automated testing:** Includes pytest tests for rejection auditing and sample-data reconciliation.

## Gold Datasets

| Dataset | Purpose |
|---|---|
| `orders_enriched` | Enriched order-level data |
| `product_revenue` | Revenue aggregated by product |
| `city_revenue` | Revenue aggregated by city |
| `customer_spending` | Customer-level spending analysis |
| `daily_revenue` | Revenue aggregated by date |
| `ranked_products` | Product ranking analytics |
| `daily_running_total` | Cumulative daily revenue |
| `orders_partitioned` | Order data organised for analytical access |

## Project Structure

```text
ecommerce-etl-pipeline/
├── data/
│   └── raw/
│       ├── customers.csv
│       ├── orders.csv
│       └── products.csv
├── src/
│   └── ecommerce_pipeline/
│       ├── __init__.py
│       ├── config.py
│       ├── ingestion.py
│       ├── transformations.py
│       ├── rejections.py
│       ├── quality.py
│       ├── gold.py
│       ├── logging_config.py
│       └── pipeline.py
├── tests/
│   ├── conftest.py
│   └── test_rejections.py
├── pyproject.toml
├── .gitignore
└── README.md
```

## Getting Started

### Prerequisites

- Python 3.11 or later
- Java compatible with the installed PySpark version
- pip

### Installation

Clone the repository:

```bash
git clone https://github.com/Soumaditya777/ecommerce-etl-pipeline.git
cd ecommerce-etl-pipeline
```

Install the project and test dependencies:

```bash
python3 -m pip install -e ".[dev]"
```

Ensure Java is installed and available:

```bash
java -version
```

### Run the Pipeline

```bash
python3 -m ecommerce_pipeline.pipeline
```

The pipeline reads the input CSV files, processes and validates the data, generates analytical datasets, and writes execution metadata.

### Run Tests

```bash
python3 -m pytest -q
```

## Sample Results

The following results were verified using the project's current sample dataset.

| Metric | Result |
|---|---:|
| Raw order rows | 17 |
| Accepted order rows | 13 |
| Rejected physical rows | 4 |
| Customers validated | 6 |
| Products validated | 5 |
| Validated Silver orders | 13 |
| Total order revenue | 26,668.0 |
| Gold datasets generated | 8 |

**Reconciliation check:**

```text
Raw order rows = Accepted order rows + Rejected physical rows
17             = 13                  + 4
```

These figures describe the sample data, not performance benchmarks.

## Output Artifacts

After a successful pipeline run, generated outputs are available under the project directory:

- `output/bronze/` — ingested data
- `output/silver/` — transformed and validated data
- `output/gold/` — analytical datasets
- `output/rejected/orders/` — rejected records and audit reasons
- `output/_SUCCESS.json` — execution status and reconciliation counts
- `logs/` — execution logs

Generated outputs and logs are excluded from Git using `.gitignore`.

## Future Enhancements

- Expand unit and integration test coverage.
- Add incremental processing and idempotent execution.
- Introduce workflow orchestration with Apache Airflow.
- Add continuous integration with GitHub Actions.
- Improve data observability and pipeline performance monitoring.

## Author

**Soumaditya Das**

GitHub: [@Soumaditya777](https://github.com/Soumaditya777)

---

*Built to demonstrate practical data engineering concepts using Python, PySpark, data quality controls, and analytical data processing.*
