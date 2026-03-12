# E-Commerce ETL Pipeline (Spark & Airflow) 🚀

A data engineering pipeline designed to extract, transform, and load (ETL) e-commerce sales data using **Apache Spark** and schedule the workflow using **Apache Airflow**.

## 🛠️ Technologies Used
* **Apache Spark (PySpark):** In-memory big data processing, data cleaning, and aggregation.
* **Apache Airflow:** Workflow orchestration and automated scheduling (DAGs).
* **Python:** Core programming language for data manipulation.

## 📂 Project Structure
* `spark_etl_processor.py`: A PySpark script that handles data extraction, data transformation (handling null values, categorizing transactions), and aggregation (calculating total sales by city).
* `ecommerce_etl_dag.py`: An Airflow DAG that schedules the ETL process to run daily, featuring task dependencies, data readiness checks, and execution logging.

## 🎯 Project Goal
This repository demonstrates foundational Data Engineering skills, focusing on modern big data processing frameworks and task orchestration tools.
