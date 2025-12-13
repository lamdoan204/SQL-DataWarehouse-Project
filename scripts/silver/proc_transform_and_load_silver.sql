
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