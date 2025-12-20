---
## Project Requirements

### Building the Data Warehouse (Data Engineering)

#### Objective
Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.

#### Specifications
- **Data Sources**: Import data from two source systems (ERP and CRM) provided as CSV files.
- **Data Quality**: Cleanse and resolve data quality issues prior to analysis.
- **Integration**: Combine both sources into a single, user-friendly data model designed for analytical queries.
- **Scope**: Focus on the latest dataset only; historization of data is not required.
- **Documentation**: Provide clear documentation of the data model to support both business stakeholders and analytics teams.
---

issue while transform and clean and load to silver layer each table
- crm_cust_info: duplicate id, unspected space, standardization of  gender and cst_marital_status columns.
- crm_prd_infor: extract cat_id, prd_key from prd_ley raw column, start_date > end_date.
- crm_salse_details: tranform sls_prd_key, check and fix unvalid sls_order_dt cast to date same for date columns, check have any order_date > ship_date or due_date, Check Bussiness Rules sale = price * quantity 
- erp_cust_az12: extract cust_key from cid column, check valid bdate (bdate < '1920-01-01' or bdate > GETDATE()), check valid gender column (distinct gen)
- 