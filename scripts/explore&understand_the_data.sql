-- liệt kê các bảng --

SELECT * from   bronze.crm_cust_info

SELECT * from bronze.crm_prd_info

SELECT * from bronze.crm_sales_details

SELECT *from bronze.erp_cust_az_12

SELECT * FROM bronze.erp_loc_a101

SELECT * from  bronze.erp_px_cat_g1v2
--
-- Checking data each table - kiểm tra dữ liệu mỗi bảng
-- crm_cust_info
SELECT * from bronze.crm_cust_info
-- đây là bảng về thông tin khách hàng
-- Check for Nulls or Duplicates in Primary Key
SELECT 
cst_id,
count(*) as COUNT
From bronze.crm_cust_info
GROUP BY cst_id 
HAVING COUNT(*) > 1
-- have some cst_id duplicate and 3 record had null value.
-- check the reason why duplicate
SELECT * from bronze.crm_cust_info WHERE cst_id = '29466'
--  > with 1 cust create multiple times with difficult date -> old record with same cst_id
-- một người tạo tài khoản nhiều lần với ngày tạo khác nhau gây ra nhiều bản ghi khác nhau 
--  với cùng một cst_id 
-- => Processing this issue: get only latest create_date.

-- CHECK THE REASON WHY NULL VALUES IN CST_ID   
SELECT * from bronze.crm_cust_info WHERE cst_id is NULL
-- => no information. Processing this issue: delete records have null value in cst_id 

-- With information of customer as String type.
-- CHECK UNWANTED SPACES
select cst_marital_status from bronze.crm_cust_info WHERE cst_marital_status != TRIM(cst_marital_status)
-- ... similar with other columns.
-- => cst_marital_status and cst_gndr dont have unwanted spaces.

