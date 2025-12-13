
INSERT into silver.crm_cust_info
(
    cst_id,
    cst_key,
    cst_first_name,
    cst_last_name,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT 
    cst_id,
    cst_key,
    trim(cst_first_name) as cst_first_name,
    trim(cst_last_name) as cst_last_name,
    case
        when UPPER(TRIM(cst_marital_status)) = 'S' then 'Single'
        when UPPER(TRIM(cst_marital_status)) = 'M' then 'Married'
        else 'n/a'
    end as cst_marital_status,
    case 
        when trim(UPPER(cst_gndr)) = 'F' then 'Female'
        when trim(upper(cst_gndr)) = 'M' then 'Male'
        else 'n/a'
    end as cst_gndr,
    cst_create_date
    FROM
        (SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last 
        from bronze.crm_cust_info
        WHERE cst_id is NOT NULL) t 

    where flag_last = 1



INSERT INTO silver.crm_prd_info (
    prd_id, 
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)select 
prd_id,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
REPLACE(SUBSTRING(prd_key, 7, len(prd_key)), '-', '_') as prd_key,
prd_nm,
Case
    when prd_cost < 0 then 0
    when prd_cost is null then 0
    else prd_cost
end as prd_cost,
case UPPER(TRIM(prd_line))
    when 'M' then 'Mountain'
    when 'R' then 'Road'
    when 'S' then 'Other Sales'
    when 'T' then 'Touring'
    else 'n/a'
end as prd_line,
CAST(prd_start_dt as DATE) as prd_start_dt,
CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 as DATE) as prd_end_dt

from bronze.crm_prd_info 