🏭 Fabric Manufacturing Analytics Platform

End-to-End Medallion Data Engineering Project using Microsoft Fabric

📌 Project Overview

This project implements a complete enterprise-style Manufacturing Analytics Platform using Microsoft Fabric.

It simulates a real-world manufacturing environment with:

Operational SQL source system

Lakehouse ingestion (Bronze)

Data quality & validation layer (Silver)

Business-ready dimensional modeling (Gold)

Fabric Warehouse star schema

Power BI semantic model

Orchestration pipelines

Governance documentation

The goal of this project was to design and implement a production-style data platform following modern data engineering best practices.

🏗 Architecture

The solution follows the Medallion Architecture pattern:

Source SQL → Bronze → Silver → Gold → Warehouse → Semantic Model → BI
🔹 Source Layer

Fabric SQL Database

Synthetic manufacturing data generation

Operational-style tables:

WorkOrders

ProductionRuns

QualityInspections

Inventory

Plants

Products

Suppliers

🥉 Bronze Layer (Raw Ingestion)

Purpose: Store raw, unmodified source data.

Characteristics:

Direct JDBC ingestion from SQL

Delta format storage

Immutable structure

Ingestion timestamp added

No transformations

Tables:

bronze_workorders

bronze_productionruns

bronze_qualityinspections

bronze_inventory

bronze_plants

bronze_products

bronze_suppliers

🥈 Silver Layer (Data Quality & Cleansing)

Purpose: Enforce data quality and business validation rules.

What was implemented:

Explicit schema casting

Null validation

Negative value checks

Date validation rules

Referential integrity enforcement

Quarantine pattern for bad data

Central DQ logging table

Run tracking with unique run_id

Each table produces:

Clean table (silver_*)

Quarantine table (silver_*_quarantine)

Logged DQ metrics (dq_issues_log)

Example validations:

PlannedQuantity must not be null

ScrapQuantity cannot be negative

ProductionRun must reference valid WorkOrder

Inventory must reference valid Product and Plant

DefectCount cannot be null or negative

🥇 Gold Layer (Business Aggregations)

Purpose: Create analytics-ready datasets.

Created fact tables:

fact_production_daily

fact_quality_daily

fact_inventory_snapshot

Created dimension tables:

dim_date

dim_product

dim_plant

Features:

Aggregated daily production metrics

Scrap & defect rate calculations

Inventory availability metrics

Business KPI derivations

🏢 Warehouse Layer

The Gold tables are modeled into a star schema in Fabric Warehouse.

Model design:

Central fact tables

Conformed dimensions

Date dimension for time intelligence

Proper relationship modeling

This enables:

High-performance querying

Clean BI modeling

Business-level reporting

📊 Semantic Model (Power BI Layer)

A Direct Lake semantic model was created on top of the Warehouse.

Features:

Star schema validation

Relationship modeling

DAX-ready measures

BI consumption layer

This layer enables:

Executive dashboards

KPI reporting

Operational insights

⚙️ Orchestration

Fabric Data Pipeline orchestrates the full data flow:

Bronze ingestion notebook

Silver DQ notebook

Gold aggregation notebook

Pipeline ensures:

Sequential execution

Dependency management

Re-runnability

Centralized processing control

🛡 Governance & Observability

Implemented governance patterns:

Structured project architecture

Data Quality logging table

Quarantine isolation strategy

Run tracking via unique run_id

Clear folder segmentation by layer

Documentation under /00_docs

Governance artifacts included:

architecture.md

governance.md

project structure documentation

🧰 Technologies Used

Microsoft Fabric

Fabric Lakehouse (Delta)

Fabric Warehouse

Spark (PySpark)

SQL

Power BI Semantic Model

Fabric Pipelines

Git / GitHub

🚀 Engineering Concepts Demonstrated

Medallion Architecture

Data Quality Framework

Quarantine Design Pattern

Referential Integrity Enforcement

Dimensional Modeling (Star Schema)

Fact & Dimension Design

KPI Derivation

Orchestration Pipelines

Observability via Run Tracking

Enterprise Repository Structure

📁 Project Structure
fabric-manufacturing-analytics/
│
├── 00_docs/
├── 01_source_sqldb/
├── 02_lakehouse/
├── 03_warehouse/
├── 04_powerbi_semantic_model/
├── 05_pipelines/
├── 06_ops_governance/
└── README.md
🎯 Business Value Simulation

This platform simulates how a manufacturing organization can:

Monitor production output

Track scrap and defect rates

Validate operational data integrity

Analyze plant-level performance

Track inventory health

Build executive KPI dashboards

👩‍💻 Author

Srinadh Reddy Gade
Data Engineering | Analytics Engineering | Microsoft Fabric

📬 Repository Purpose

This project was built as a portfolio demonstration of end-to-end data engineering capability in Microsoft Fabric, showcasing architectural thinking, engineering rigor, governance awareness, and BI integration.



## 📸 Visual Walkthrough

See detailed architecture screenshots here:

[Project Visual Walkthrough](00_docs/visual_walkthrough.md)