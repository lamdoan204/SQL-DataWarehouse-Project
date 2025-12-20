use DataWarehouse
GO

if OBJECT_ID('bronze.crm_cust_info', 'U') is NOT NULL
    DROP TABLE bronze.crm_cust_info
CREATE TABLE bronze.crm_cust_info(
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_first_name NVARCHAR(50),
    cst_last_name NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date DATETIME
);
GO
if OBJECT_ID('bronze.crm_prd_info', 'U') is NOT NULL
    DROP TABLE bronze.crm_prd_info
CREATE TABLE bronze.crm_prd_info(
    prd_id INT,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt DATETIME
);
GO
if OBJECT_ID('bronze.crm_sales_details', 'U') is NOT NULL
    DROP TABLE bronze.crm_sales_details
CREATE TABLE bronze.crm_sales_details(
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt int,
    sls_ship_dt int,
    sls_due_dt INT,
    sls_sales FLOAT,
    sls_quantity INT,
    sls_price FLOAT
);
GO
if OBJECT_ID('bronze.erp_cust_az_12', 'U') is NOT NULL
    DROP TABLE bronze.erp_cust_az_12
CREATE TABLE bronze.erp_cust_az_12(
    cid NVARCHAR(50),
    bdate DATETIME,
    gen NVARCHAR(50)
);
GO
if OBJECT_ID('bronze.erp_loc_a101', 'U') is NOT NULL
    DROP TABLE bronze.erp_loc_a101
CREATE TABLE bronze.erp_loc_a101(
    cid NVARCHAR(50),
    cntry NVARCHAR(50)
);
GO
if OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') is NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2
CREATE TABLE bronze.erp_px_cat_g1v2(
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50),
);