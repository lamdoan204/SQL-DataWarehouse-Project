
-- EXEC silver.load_silver
/*
    THIS PROCEDURE: TRANSFORM, CLEAN DATA FROM BRONZE LAYER TO SILVER LAYER
    PURPOSE:
    - TRUNCATE ALL TABLE IN SILVER LAYER 
    - TRANSFORM EACH COLUMN IN TABLE IN BRONZE LAYER THEN LOAD TO TABLE IN SILVER LAYER
    - USED FUNCTIONS: CAST(), CASE WHEN, NULLIF(), ISNULL(), UPPER(), TRIM(), REPLACE(), SUBSTRING(), ROW NUMBER() OVER(PARTITION BY <> ORDER BY <>) 
*/
CREATE or ALTER PROCEDURE silver.load_silver as 
    BEGIN
        DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
    BEGIN TRY
        PRINT '========================================================================='
        PRINT 'STARTING PROCEDURE TRANSFORM DATA TO SILVER'
        SET @batch_start_time = GETDATE()
        PRINT '========================================================================='

        PRINT '================================================================'
        SET @start_time =GETDATE()
        PRINT 'STARTING LOAD TABLE: silver.crm_cust_info'
        PRINT '================================================================'

        PRINT 'Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info
        PRINT 'Inserting Data Into Table: silver.crm_cust_info'
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
        PRINT '================================================================'
        PRINT 'END LOAD TABLE: silver.crm_cust_info'
        SET @end_time = GETDATE()
        PRINT 'lOAD DURATION: ' + CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' SECONDS' 
        PRINT '================================================================'
        -------------------------------------------------------------------
        PRINT '================================================================'
        SET @start_time = GETDATE()
        PRINT 'STARTING LOAD TABLE: silver.crm_prd_info'
        PRINT '================================================================'

        PRINT 'Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info
        PRINT 'Inserting Data Into Table: silver.crm_prd_info'
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
            when prd_cost < 0 or prd_cost is null then 0
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
        PRINT '================================================================'
        PRINT 'END LOAD TABLE: silver.crm_prd_info'
        SET @end_time = GETDATE()
        PRINT 'lOAD DURATION: ' + CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' SECONDS' 
        PRINT '================================================================'

        ------------------------------------------------------------------------
        PRINT '================================================================'
        SET @start_time =GETDATE()
        PRINT 'STARTING LOAD TABLE: silver.crm_sales_details'
        PRINT '================================================================'

        PRINT 'Truncating Table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details
        PRINT 'Inserting Data Into Table: silver.crm_sales_details'

        insert into silver.crm_sales_details(
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_quantity,
            sls_price,
            sls_sales
        )   
        SELECT
            sls_ord_num,
            REPLACE(sls_prd_key, '-', '_') as sls_prd_key,
            sls_cust_id,

            case 
                when sls_order_dt <= 0 or LEN(sls_order_dt) != 8 then null
                else cast( CAST(sls_order_dt as nvarchar) as date)
            end as sls_order_dt,

            case 
                when sls_ship_dt <= 0 or LEN(sls_ship_dt) != 8 then null
                else cast( CAST(sls_ship_dt as nvarchar) as date)
            end as sls_ship_dt,

            case 
                when sls_due_dt <= 0 or LEN(sls_due_dt) != 8 then null
                else cast( CAST(sls_due_dt as nvarchar) as date)
            end as sls_due_dt,
            sls_quantity,
            case 
                when sls_price is null or sls_price <=0 then sls_sales / sls_quantity
                else sls_price
            end as sls_price,
            case
                when sls_sales <= 0 or sls_sales is null or sls_sales != sls_quantity * abs(sls_price) then sls_price * sls_quantity
                else sls_sales
            end as sls_sales

        from bronze.crm_sales_details

        PRINT '================================================================'
        PRINT 'END LOAD TABLE: silver.crm_sales_details'
        SET @end_time = GETDATE()
        PRINT 'lOAD DURATION: ' + CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' SECONDS' 
        PRINT '================================================================'
        ---------------------------------------------------------------------------
        PRINT '================================================================'
        SET @start_time =GETDATE()
        PRINT 'STARTING LOAD TABLE: silver.crm_prd_info'
        PRINT '================================================================'

        PRINT 'Truncating Table: silver.erp_cust_az_12';
        TRUNCATE TABLE silver.erp_cust_az_12
        PRINT 'Inserting Data Into Table: silver.erp_cust_az_12'

        insert into silver.erp_cust_az_12(
            cid,
            bdate,
            gen
        )
        SELECT
        case when cid like 'NAS%' then SUBSTRING(cid, 4, LEN(cid))
            else cid
        end as cid,

        case when bdate > GETDATE() then null
            else bdate
        end as bdate, 

        case when UPPER(TRIM(gen)) in ('FEMALE', 'F') THEN 'Female'
            when UPPER(TRIM(gen)) in ('M', 'MALE') then 'Male'
            else 'n/a'
        end as gen
        FROM bronze.erp_cust_az_12

        PRINT '================================================================'
        PRINT 'END LOAD TABLE: silver.erp_cust_az_12'
        SET @end_time = GETDATE()
        PRINT 'lOAD DURATION: ' + CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' SECONDS' 
        PRINT '================================================================'
        -----------------------------------------------------------------------------
        PRINT '================================================================'
        SET @start_time =GETDATE()
        PRINT 'STARTING LOAD TABLE: silver.erp_loc_a101'
        PRINT '================================================================'


        PRINT 'Truncating Table: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101
        PRINT 'Inserting Data Into Table: silver.erp_loc_a101'

        insert into silver.erp_loc_a101(cid, cntry)

        SELECT
        REPLACE(cid, '-', '') as cid,
        case when UPPER(TRIM(cntry)) = 'DE' then 'Germany'
            when UPPER(TRIM(cntry)) in ('US', 'USA', 'UNITED STATES') THEN 'United States'
            when TRIM(cntry) = '' or cntry is null then 'n/a'
            else cntry
        end as cntry
        from bronze.erp_loc_a101

        PRINT '================================================================'
        PRINT 'END LOAD TABLE: silver.erp_loc_a101'
        SET @end_time = GETDATE()
        PRINT 'lOAD DURATION: ' + CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' SECONDS' 
        PRINT '================================================================'
        -----------------------------------------------------------------------------
        PRINT '================================================================'
        SET @start_time =GETDATE()
        PRINT 'STARTING LOAD TABLE: silver.erp_px_cat_g1v2'
        PRINT '================================================================'
        PRINT 'Truncating Table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2
        PRINT 'Inserting Data Into Table: silver.erp_px_cat_g1v2'

        insert into silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
        select 
            case when id = 'CO_PD' then 'CO_PE'
                else id
            end as id,
            cat,
            subcat,
            maintenance 
        from 
        bronze.erp_px_cat_g1v2
        PRINT '================================================================'
        PRINT 'END LOAD TABLE: silver.erp_px_cat_g1v2'
        SET @end_time = GETDATE()
        PRINT 'lOAD DURATION: ' + CAST(DATEDIFF(SECOND,@start_time, @end_time) AS NVARCHAR) + ' SECONDS' 
        PRINT '================================================================'

        PRINT '========================================================================='
        PRINT 'End PROCEDURE TRANSFORM DATA TO SILVER LAYER'
        SET @batch_end_time = GETDATE()
        PRINT 'BATCH lOAD DURATION: ' + CAST(DATEDIFF(SECOND,@batch_end_time, @batch_start_time) AS NVARCHAR) + ' SECONDS'
        PRINT '========================================================================='
        
    END TRY
    BEGIN CATCH
        PRINT '======================================================================'
        PRINT 'AN ERROR OCCURED DURING LOAD AND TRANSFORM TO SILVER LAYER !'
        PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE();
        PRINT 'ERROR MESSAGE: '+  CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT '======================================================================'
    END CATCH
END


