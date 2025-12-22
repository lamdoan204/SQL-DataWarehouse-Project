
-- create view dimension customers
GO
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
CREATE VIEW gold.dim_customers AS
SELECT 
ROW_NUMBER() over (ORDER BY ci.cst_id) as customer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_first_name as first_name,
ci.cst_last_name as last_name,
case 
    when ci.cst_gndr != 'n/a' then ci.cst_gndr
     else coalesce(ca.gen, 'n/a')
end as gender,
ca.bdate as birthday,
la.cntry as country,
ci.cst_marital_status marital_status,
ci.cst_create_date

from silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az_12 ca 
    on ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la 
    on ci.cst_key = la.cid


-- create view dimension products

GO
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
CREATE VIEW gold.dim_products  as
SELECT
    ROW_NUMBER() over (order by pi.prd_key) as product_key,
    pi.prd_id as product_id,
    pi.prd_key as product_number,
    pi.prd_nm as product_name,
    pi.cat_id as category_id,
    pcg.cat as category,
    pcg.subcat as sub_category,
    pi.prd_cost as cost,
    pi.prd_line as line,
    pi.prd_start_dt as start_date,
    pcg.maintenance as maintenance
from silver.crm_prd_info pi  
LEFT JOIN silver.erp_px_cat_g1v2 pcg on pi.cat_id = pcg.id
where pi.prd_end_dt is NULL -- get the newest product in history

-- create view fact sales
GO
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO
CREATE VIEW gold.fact_sales AS
SELECT
sd.sls_ord_num as order_number,
dc.customer_key,
dp.product_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as shipping_date,
sd.sls_due_dt as  due_date,
sd.sls_sales as sales_amount,
sd.sls_quantity as quantity,
sd.sls_price as price,
ABS((dp.cost - sd.sls_price)*sd.sls_quantity )as profit

From silver.crm_sales_details sd
    LEFT JOIN gold.dim_customers dc on sd.sls_cust_id = dc.customer_id
    LEFT JOIN gold.dim_products dp on sd.sls_prd_key = dp.product_number

