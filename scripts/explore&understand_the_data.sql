SELECT
cst_id, 
count(cst_id)
from silver.crm_cust_info
GROUP by cst_id 
HAVING COUNT(cst_id) > 1 and cst_id is NULL

select * from silver.crm_cust_info
where cst_last_name != TRIM(cst_last_name)