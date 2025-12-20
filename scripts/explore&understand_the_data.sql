SELECT * from bronze.erp_px_cat_g1v2 WHERE
trim(id) != id or trim(cat) != cat or TRIM(subcat) != subcat or trim(maintenance) != maintenance


select * from silver.erp_px_cat_g1v2

SELECT distinct id from bronze.erp_px_cat_g1v2 where id = 'CO_PD'