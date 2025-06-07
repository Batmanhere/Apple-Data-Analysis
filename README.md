# 🍏 Apple Analysis ETL Pipeline

This repository showcases a modular and scalable ETL pipeline developed for analyzing Apple Inc. data using PySpark on Databricks. The pipeline follows object-oriented design principles and is built to handle various data formats (CSV, Parquet, Delta) while maintaining clean and reusable architecture through the Factory Pattern.

---

## 🚀 Project Overview

The goal of this project is to build a robust ETL (Extract, Transform, Load) pipeline that ingests and processes Apple’s stock and financial data to enable business-driven analysis. The pipeline was designed for scalability and maintainability in a cloud-based environment using Databricks and PySpark.

---

## 🛠️ Tools & Technologies

- **Platform:** Azure Databricks
- **Language:** Python,PySpark
- **Processing Engine:** Spark
- **File Formats:** CSV, Parquet, Delta

---

## 🔧 Architecture & Components

The pipeline is modularized into four core components, implemented using object-oriented principles and the Factory Pattern:

### 🧾 Reader Class
- Ingests data from multiple file formats (CSV, Parquet, Delta)
- Automatically identifies and handles file schemas and partitions

### 🧪 Extractor Class
- Extracts required columns from raw datasets
- Performs basic filtering and validation

### 🔄 Transformer Class
- Applies business rules and data transformations
- Handles missing values, aggregations, and enrichments

### 📤 Loader Class
- Writes the processed data into the target destination (Delta Lake or Parquet)
- Supports data partitioning and overwrite/append modes

---

## ⚙️ ETL Flow Diagram

```text
        +-------------+      +-------------+      +-------------+      +-------------+
        |   Reader    | ---> |  Extractor  | ---> | Transformer | ---> |   Loader    |
        +-------------+      +-------------+      +-------------+      +-------------+
             |                    |                    |                    |
         Raw Data           Extracted Data       Transformed Data       Loaded Data
       (CSV, Parquet)                                                  (Delta Tables)
