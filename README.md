# Week 2 ETL & EDA Project

This project implements an end-to-end ETL pipeline followed by exploratory data analysis (EDA).
Raw data is cleaned, validated, transformed into analytics-ready tables, and summarized with
visual insights.

---

## Project Structure

data/

* raw/                Raw CSV input files
* processed/          Cleaned and analytics-ready Parquet files

src/

* bootcamp_data/      ETL source code (I/O, transforms, quality checks, joins, etl)

scripts/

* run_etl.py          Entry point to run the ETL pipeline

notebooks/

* eda.ipynb           Exploratory Data Analysis notebook

reports/

* figures/            Saved plots generated during EDA
* summary.md          Written summary of findings

README.md

---

## Setup

Install project dependencies:

```bash
uv pip install -r requirements.txt
```

---

## Run ETL

Run the full ETL pipeline:

```bash
uv run python -m scripts.run_etl
```

This will generate the following files in `data/processed/`:

* orders_clean.parquet
* users.parquet
* analytics_table.parquet
* _run_meta.json

---

## Exploratory Data Analysis (EDA)

Open the notebook:

```
notebooks/eda.ipynb
```

Notes:

* The notebook reads **only** from `data/processed/`
* Plots are saved to `reports/figures/`

---

## Key Findings

* **Order Amount Distribution**
  Order amounts are right-skewed with a small number of extreme values.
  Winsorization at the 1st and 99th percentiles reduced the impact of outliers.

* **Orders by Country**
  Most orders come from Saudi Arabia (sa), followed by the UAE (ae), indicating a strong
  concentration in the Saudi market.

* **Order Trends Over Time**
  Order volume shows a stable daily pattern with no extreme spikes.

---

## Data Quality Notes

* Missing timestamps were preserved as `NaT` values.
* Outliers were handled using winsorization rather than row removal.
* Joins between orders and users achieved high coverage.
