# 🏢 Data Warehouse and Analytics Project

Welcome to the **Data Warehouse and Analytics Project** 🚀  
This project presents a full end-to-end data warehousing and analytics solution using industry best practices. It simulates a real-world business scenario — from ingesting raw data to delivering meaningful business insights — and is designed to showcase skills in data engineering, modeling, and analytics.

---

## 🏗️ Data Architecture

The architecture is built using the **Medallion Architecture** model, consisting of three main layers:

![Data Architecture](docs/data_architecture.png)

1. **Bronze Layer** – Stores raw, unprocessed data as-is from the source systems. Data is ingested from CSV files into a SQL Server database.
2. **Silver Layer** – Cleanses and standardizes the data to ensure consistency and quality for analysis.
3. **Gold Layer** – Delivers business-ready, aggregated data in a star schema format optimized for reporting and analytics.

---

## 🔍 Project Overview

The project covers the following key components:

- **Data Architecture**: Designing a modern, layered architecture using Bronze, Silver, and Gold tiers.
- **ETL Pipelines**: Building efficient Extract, Transform, Load (ETL) pipelines using SQL.
- **Data Modeling**: Creating normalized and denormalized data models, including fact and dimension tables.
- **Analytics & Reporting**: Generating insights using SQL queries and analytical dashboards.

🎯 This project demonstrates practical skills in:
- Data Engineering  
- SQL Development  
- ETL Process Design  
- Data Modeling  
- Business Intelligence & Reporting  

---

## 🛠️ Tools & Technologies

- **SQL Server Express** – Relational database engine for hosting the warehouse  
- **SQL Server Management Studio (SSMS)** – Interface for querying and managing SQL Server databases  
- **DrawIO** – Diagramming tool for architecture and data flow visualization  
- **GitHub** – Version control and project collaboration  
- **Notion** – Documentation and project management (optional)

---

## 📁 Repository Structure

```plaintext
data-warehouse-project/
│
├── datasets/                           # Raw datasets (ERP and CRM)
│
├── docs/                               # Documentation and diagrams
│   ├── etl.drawio                      # ETL flow diagrams
│   ├── data_architecture.drawio        # Project architecture
│   ├── data_catalog.md                 # Field descriptions and metadata
│   ├── data_flow.drawio                # Data flow design
│   ├── data_models.drawio              # Star schema and dimensional models
│   ├── naming-conventions.md           # Naming conventions for all data assets
│
├── scripts/                            # SQL scripts used in the project
│   ├── bronze/                         # Raw data ingestion scripts
│   ├── silver/                         # Data transformation and cleansing
│   ├── gold/                           # Business-ready model creation
│
├── tests/                              # Data quality and test queries
│
├── README.md                           # Project overview and guide
├── LICENSE                             # Project license
├── .gitignore                          # Git ignore rules
└── requirements.txt                    # Dependencies (if any)
