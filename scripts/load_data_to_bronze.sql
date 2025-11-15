
CREATE or alter PROCEDURE bronze.load_bronze as 
BEGIN
    BEGIN TRY
        PRINT '===========================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '===========================================================';
        PRINT '---------------------------------------'
        PRINT 'Loading CRM Tables';
        PRINT '---------------------------------------'
        PRINT '-------------------------'
        PRINT 'Loading bronze.crm_cust_info'
        PRINT '-------------------------'

        TRUNCATE TABLE bronze.crm_cust_info
        BULK INSERT bronze.crm_cust_info
        FROM 'D:\Projects for CV\SQL-DataWarehouse-Project\datasets\source_crm\cust_info.csv'
        with (
            FIRSTROW = 2,
            fieldterminator = ',',
            tablock
        );
        PRINT '-------------------------'
        PRINT 'Loading bronze.crm_prd_info'
        PRINT '-------------------------'

        TRUNCATE TABLE bronze.crm_prd_info
        BULK INSERT bronze.crm_prd_info
        FROM 'D:\Projects for CV\SQL-DataWarehouse-Project\datasets\source_crm\prd_info.csv'
        WITH(
            FIRSTROW = 2,
            fieldterminator = ',',
            tablock
        );
        PRINT '-------------------------'
        PRINT 'Loading bronze.crm_sales_details'
        PRINT '-------------------------'

        TRUNCATE TABLE bronze.crm_sales_details
        BULK INSERT bronze.crm_sales_details
        from 'D:\Projects for CV\SQL-DataWarehouse-Project\datasets\source_crm\sales_details.csv'
        with(
            FIRSTROW = 2,
            fieldterminator = ',',
            tablock
        );
        PRINT '==========================================='
        PRINT 'Loading CRM Tables'
        PRINT '==========================================='

        PRINT '-------------------------'
        PRINT 'Loading bronze.erp_cust_az_12'
        PRINT '-------------------------'
        TRUNCATE TABLE bronze.erp_cust_az_12
        BULK INSERT bronze.erp_cust_az_12
        FROM 'D:\Projects for CV\SQL-DataWarehouse-Project\datasets\source_erp\CUST_AZ12.csv'
        WITH(
            FIRSTROW = 2,
            fieldterminator = ',',
            tablock
        );
        PRINT '-------------------------'
        PRINT 'Loading bronze.erp_loc_a101'
        PRINT '-------------------------'
        TRUNCATE TABLE bronze.erp_loc_a101
        BULK INSERT bronze.erp_loc_a101
        from 'D:\Projects for CV\SQL-DataWarehouse-Project\datasets\source_erp\LOC_A101.csv'
        with(
            FIRSTROW = 2,
            fieldterminator = ',',
            tablock
        );
        PRINT '-------------------------'
        PRINT 'Loading bronze.erp_px_cat_g1v2'
        PRINT '-------------------------'
        TRUNCATE TABLE bronze.erp_px_cat_g1v2
        BULK INSERT bronze.erp_px_cat_g1v2
        from 'D:\Projects for CV\SQL-DataWarehouse-Project\datasets\source_erp\PX_CAT_G1V2.csv'
        with(
            FIRSTROW = 2,
            fieldterminator = ',',
            tablock
        );
    END TRY
    BEGIN CATCH
            PRINT '=========================================================='
            PRINT 'Error occured during loading bronze layer !'
            PRINT 'Error message' +  ERROR_MESSAGE();
            PRINT 'Error message' + ERROR_NUMBER(); 
            PRINT '=========================================================='
    END CATCH
END

