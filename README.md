# STEDI Human Balance – Data Lakehouse Platform

## 📌 Project Overview
This project implements a **modern Data Lakehouse architecture** on AWS for the **STEDI Human Balance** dataset.  
The goal is to ingest raw sensor and customer data, enforce data privacy and quality, and produce an **analytics‑ and machine‑learning–ready dataset** using serverless cloud services and industry best practices.

The solution combines the **flexibility of a data lake** with the **governance and analytics capabilities of a data warehouse**, following a **Medallion (Bronze–Silver–Gold) architecture**.

---

## 🏗️ Architecture Summary

**Core Technologies**
- **Amazon S3** – Scalable object storage (data lake)
- **AWS Glue** – Serverless ETL engine
- **AWS Glue Data Catalog** – Centralized metadata & schema management
- **Amazon Athena** – Serverless SQL analytics
- **Apache Spark (Glue)** – Distributed data processing
- **Apache Parquet** – Columnar storage format for analytics

**Key Design Principles**
- Serverless & cost‑efficient
- Open file formats (no vendor lock‑in)
- Privacy‑aware data transformations
- Decoupled storage and compute
- SQL‑first analytics

---

## 🧠 Data Lakehouse Design

This project follows a **Medallion Architecture**, a standard lakehouse pattern used in production environments.

### 🥉 Bronze Layer – Landing (Raw Data)
Raw JSON data ingested without modification.

**Tables**
- `customer_landing` (956 rows)
- `accelerometer_landing` (81,273 rows)
- `step_trainer_landing` (28,680 rows)

**Location**