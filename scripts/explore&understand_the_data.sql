select 
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




SELECT * from bronze.crm_prd_info where prd_start_dt > prd_end_dt




select * from bronze.crm_cust_info