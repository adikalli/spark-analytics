# Sales Data Generator Utility

A lightweight Python utility to generate synthetic sales data for testing data pipelines, ETL workflows, and analytics projects.

This project is designed to simulate small-scale transactional data and can be easily extended for large-scale data engineering experiments.

---

## Features

- Generate configurable number of rows (default: 1000)
- Deterministic output using random seed
- Clean modular utility structure
- CLI support
- Zero external dependencies
- Ready for ETL / Spark / Warehouse ingestion


---

## Schema

| Column       | Type    | Description                     |
|-------------|---------|---------------------------------|
| order_id     | int     | Unique order identifier         |
| customer_id  | int     | Customer identifier             |
| amount       | float   | Order value                     |
| country      | string  | Country code (IN, US, DE, etc.) |

---

## Usage

Clone the repo and start running.

### Default (1000 rows)

```bash
python main.py
```

### Reproducible Dataset
By default output location is `data/input/sales_small.csv`

```bash
python main.py --rows 5000 --seed 42 --output data/input/custom_file.csv
```



