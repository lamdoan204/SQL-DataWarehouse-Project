
/*
    - Stored procedured: Load Bronze Layer (Source - Bronze Layer)
    Script Purpose:
    This stored procedure loads data into the bronze schema form external scv files.
    It performs the following actions:
    -   Truncates the bronze tables before loading data.
    -   Using the 'Bulk insert' command to load data from csv files to bronze tables.
*/
CREATE or alter PROCEDURE bronze.load_bronze as 
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
    BEGIN TRY
        PRINT '===========================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '===========================================================';
        SET @batch_start_time = GETDATE()
        PRINT '---------------------------------------'
        PRINT 'Loading CRM Tables';
        PRINT '---------------------------------------'
        PRINT '-------------------------'
        PRINT 'Loading bronze.crm_cust_info'
        PRINT '-------------------------'

        SET @start_time = GETDATE()
        TRUNCATE TABLE bronze.crm_cust_info
        BULK INSERT bronze.crm_cust_info
        FROM 'D:\Projects for CV\SQL-DataWarehouse-Project\datasets\source_crm\cust_info.csv'
        with (
            FIRSTROW = 2,
            fieldterminator = ',',
            tablock
        );
        SET @end_time = GETDATE()
        PRINT '-----------'
        PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) as NVARCHAR) + ' seconds'
        PRINT '-----------'

        PRINT '-------------------------'
        PRINT 'Loading bronze.crm_prd_info'
        PRINT '-------------------------'
        SET @start_time = GETDATE()
        TRUNCATE TABLE bronze.crm_prd_info
        BULK INSERT bronze.crm_prd_info
        FROM 'D:\Projects for CV\SQL-DataWarehouse-Project\datasets\source_crm\prd_info.csv'
        WITH(
            FIRSTROW = 2,
            fieldterminator = ',',
            tablock
        );
        PRINT '-----------'
        PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) as NVARCHAR) + ' seconds'
        PRINT '-----------'

        PRINT '-------------------------'
        PRINT 'Loading bronze.crm_sales_details'
        PRINT '-------------------------'
        SET @start_time = GETDATE()
        TRUNCATE TABLE bronze.crm_sales_details
        BULK INSERT bronze.crm_sales_details
        from 'D:\Projects for CV\SQL-DataWarehouse-Project\datasets\source_crm\sales_details.csv'
        with(
            FIRSTROW = 2,
            fieldterminator = ',',
            tablock
        );
        PRINT '-----------'
        PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) as NVARCHAR) + ' seconds'
        PRINT '-----------'

        PRINT '==========================================='
        PRINT 'Loading CRM Tables'
        PRINT '==========================================='

        PRINT '-------------------------'
        PRINT 'Loading bronze.erp_cust_az_12'
        PRINT '-------------------------'
        SET @start_time = GETDATE()

        TRUNCATE TABLE bronze.erp_cust_az_12
        BULK INSERT bronze.erp_cust_az_12
        FROM 'D:\Projects for CV\SQL-DataWarehouse-Project\datasets\source_erp\CUST_AZ12.csv'
        WITH(
            FIRSTROW = 2,
            fieldterminator = ',',
            tablock
        );
        PRINT '-----------'
        PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) as NVARCHAR) + ' seconds'
        PRINT '-----------'

        PRINT '-------------------------'
        PRINT 'Loading bronze.erp_loc_a101'
        PRINT '-------------------------'
        SET @start_time = GETDATE()
        TRUNCATE TABLE bronze.erp_loc_a101
        BULK INSERT bronze.erp_loc_a101
        from 'D:\Projects for CV\SQL-DataWarehouse-Project\datasets\source_erp\LOC_A101.csv'
        with(
            FIRSTROW = 2,
            fieldterminator = ',',
            tablock
        );
        PRINT '-----------'
        PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) as NVARCHAR) + ' seconds'
        PRINT '-----------'
        PRINT '-------------------------'
        PRINT 'Loading bronze.erp_px_cat_g1v2'
        PRINT '-------------------------'
        SET @start_time = GETDATE()
        TRUNCATE TABLE bronze.erp_px_cat_g1v2
        BULK INSERT bronze.erp_px_cat_g1v2
        from 'D:\Projects for CV\SQL-DataWarehouse-Project\datasets\source_erp\PX_CAT_G1V2.csv'
        with(
            FIRSTROW = 2,
            fieldterminator = ',',
            tablock
        );
        PRINT '-----------'
        PRINT 'Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) as NVARCHAR) + ' seconds'
        PRINT '-----------'
        SET @batch_end_time = GETDATE()
        PRINT '===================================================================='
        PRINT 'Loading Bronze Layer is completed !!!!!'
        PRINT 'Batch Load Duration: '+ CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) as NVARCHAR) + ' seconds'
    END TRY
    BEGIN CATCH
            PRINT '=========================================================='
            PRINT 'Error occured during loading bronze layer !'
            PRINT 'Error message: ' +  ERROR_MESSAGE();
            PRINT 'Error message: ' + CAST(ERROR_NUMBER() as NVARCHAR); 
            PRINT '=========================================================='
    END CATCH
END

-- EXEC bronze.load_bronze

