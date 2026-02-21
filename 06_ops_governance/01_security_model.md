Overview:

This project implements role-based access control (RBAC) principles aligned with enterprise Microsoft Fabric deployments.

Access is separated by data layer and user persona to ensure proper data governance and security boundaries.

| Role               | Access Scope         | Permissions  |
| ------------------ | -------------------- | ------------ |
| Data Engineer      | Bronze, Silver, Gold | Read/Write   |
| Analytics Engineer | Silver, Gold         | Read         |
| BI Developer       | Gold, Warehouse      | Read         |
| Business User      | Warehouse Views      | Read-only    |
| Platform Admin     | All Layers           | Full Control |

Layer-Based Access Strategy:

Bronze (Raw Data):
-Restricted to Data Engineering team only
-No direct BI or business access
-Prevents exposure of unvalidated data

Silver (Validated Data):
-Accessible to Data Engineers & Analytics Engineers
-Contains cleaned and quarantined data
-Business users restricted

Gold (Curated Data):
-Accessible to BI Developers
-Optimized for reporting & performance

Warehouse & Views:
-Business users access only curated BI views
-No direct access to Lakehouse tables

Security Controls Applied:

Workspace-level RBAC
SQL endpoint permission control
Logical separation of medallion layers
Quarantine isolation for invalid records



