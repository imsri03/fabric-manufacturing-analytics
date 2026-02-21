Data Quality & Validation Framework:

Architecture: Data Quality enforcement occurs in the Silver layer as part of the Medallion Architecture.

Bronze → Raw ingestion
Silver → Validation + cleansing
Gold → Curated analytics

DQ Rule Categories Implemented : 1. Completeness - PlannedQuantity NOT NULL, DefectCount NOT NULL

2. Validity: No negative quantities, ScrapQuantity >= 0, QuantityOnHand >= 0

3. Referential Integrity: WorkOrderID must exist, ProductionRunID must exist, ProductID & PlantID must exist

Quarantine Strategy: 
 Invalid records are:

* Redirected to silver_*_quarantine
* Tagged with dq_reason
* Timestamped with quarantined_at

Valid records move to Silver curated tables.

Audit Logging : 

Every pipeline run logs:

* run_id
* rule_name
* issue_count
* timestamp

Stored in:

* dq_issues_log

This enables:

* Historical tracking
* DQ trend analysis
* Operational monitoring