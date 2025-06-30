/*
==========================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
==========================================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
     - Truncates the bronze tables before loading data.
     -Uses 'BULK INSERT' command to load data from csv files to bronze tables.

Parameters:
    None.
  This stored procedure does not accept any parameters or return any values.

Usage examples:
     EXEC bronze.load_bronze;
*/
EXEC bronze.load_bronze

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
BEGIN TRY
   SET @batch_start_time = GETDATE();
   PRINT '======================================================'
   PRINT 'Loading Bronze Layer'
   PRINT '======================================================'

   PRINT '------------------------------------------------------'
   PRINT 'Loading CRM Tables'
   PRINT '------------------------------------------------------'

   SET @start_time = GETDATE();
PRINT '>>Truncating Table:bronze.crm_cust_info'
TRUNCATE TABLE bronze.crm_cust_info;

PRINT '>>Insert Data into:bronze.crm_cust_info'
BULK INSERT bronze.crm_cust_info
FROM 'D:\Projects - Data Science\DataWarehouse Project\datasets\source_crm\cust_info.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK

);
SET @end_time = GETDATE();
PRINT '>>Load Furation: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
PRINT '----------------------------------------'
---SELECT * FROM bronze.crm_cust_info

---Load data into prd_info table-------
PRINT '>>Truncating Table:bronze.crm_prd_info'
TRUNCATE TABLE bronze.crm_prd_info;

PRINT '>>Insert Data into:bronze.crm_prd_info'
BULK INSERT bronze.crm_prd_info
FROM 'D:\Projects - Data Science\DataWarehouse Project\datasets\source_crm\prd_info.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK

);
SET @end_time = GETDATE();
PRINT '>>Load Furation: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
PRINT '----------------------------------------'

--------SELECT * FROM bronze.crm_prd_info

------Load data into crm_sales_details table--------
PRINT '>>Truncating Table:bronze.crm_sales_details'
TRUNCATE TABLE bronze.crm_sales_details;

PRINT '>>Insert Data into:bronze.crm_sales_details'
BULK INSERT bronze.crm_sales_details
FROM 'D:\Projects - Data Science\DataWarehouse Project\datasets\source_crm\sales_details.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK

);

SET @end_time = GETDATE();
PRINT '>>Load Furation: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
PRINT '----------------------------------------'

------------SELECT * FROM bronze.crm_sales_details

   PRINT '------------------------------------------------------'
   PRINT 'Loading ERP Tables'
   PRINT '------------------------------------------------------'

----Load data into erp_cust_az12--------
PRINT '>>Truncating Table:bronze.erp_cust_az12'
TRUNCATE TABLE bronze.erp_cust_az12;

PRINT '>>Insert Data into:bronze.erp_cust_az12'
BULK INSERT bronze.erp_cust_az12
FROM 'D:\Projects - Data Science\DataWarehouse Project\datasets\source_erp\CUST_AZ12.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK

);
SET @end_time = GETDATE();
PRINT '>>Load Furation: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
PRINT '----------------------------------------'

--------------SELECT * FROM bronze.erp_cust_az12

------Load data into erp_loc_a101 table----------
PRINT '>>Truncating Table:bronze.erp_loc_a101'
TRUNCATE TABLE bronze.erp_loc_a101;

PRINT '>>Insert Data into:bronze.erp_loc_a101'
BULK INSERT bronze.erp_loc_a101
FROM 'D:\Projects - Data Science\DataWarehouse Project\datasets\source_erp\LOC_A101.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK

);
SET @end_time = GETDATE();
PRINT '>>Load Furation: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
PRINT '----------------------------------------'
--------------SELECT * FROM bronze.erp_loc_a101

-------Load data into erp_px_cat_g1v2------------------
PRINT '>>Truncating Table:bronze.erp_px_cat_g1v2'
TRUNCATE TABLE bronze.erp_px_cat_g1v2;

PRINT '>>Insert Data into:bronze.erp_px_cat_g1v2'
BULK INSERT bronze.erp_px_cat_g1v2
FROM 'D:\Projects - Data Science\DataWarehouse Project\datasets\source_erp\PX_CAT_G1V2.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK

);
SET @end_time = GETDATE();
PRINT '>>Load Furation: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
PRINT '----------------------------------------'

SET @batch_end_time = GETDATE();
PRINT '============================================================'
PRINT 'Loading Bronze Layer is Completed';
PRINT '  -Total Load Duration: ' + CAST(DATEDIFF(SECOND,@batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
PRINT '============================================================'

------------------SELECT * FROM bronze.erp_px_cat_g1v2
END TRY
BEGIN CATCH
    PRINT '============================================================='
	PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
	PRINT 'Error Message'+ ERROR_MESSAGE();
	PRINT 'Error Message'+ CAST (ERROR_NUMBER() AS NVARCHAR);
	PRINT 'Error Message'+ CAST (ERROR_STATE() AS NVARCHAR);
	PRINT '============================================================='
END CATCH

END
