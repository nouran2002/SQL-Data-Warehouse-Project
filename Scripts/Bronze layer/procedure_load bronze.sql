/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze_load_bronze AS
BEGIN
DECLARE @start_time DATETIME , @end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT'=================================================================';
		PRINT'Loading Bronze Layer';
		PRINT'=================================================================';

		PRINT'************************************************';
		PRINT'Loading crm tables';
		PRINT'************************************************';
		SET @start_time = GETDATE();
		PRINT'>> Truncating: crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
	
		PRINT'>> Inserting data into: crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'E:\Google data analytics certificate\Data warwhouse\sql data warehouse my_project\Datasets\source_crm\cust_info.CSV'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',' ,
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR) + ' seconds';
		PRINT'-------------------';
		SET @start_time = GETDATE();
		PRINT'>> Truncating: crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT'>> Inserting data into: crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'E:\Google data analytics certificate\Data warwhouse\sql data warehouse my_project\Datasets\source_crm\prd_info.CSV'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',' ,
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR) + ' seconds';
		PRINT'-------------------';

		SET @start_time = GETDATE();
		PRINT'>> Truncating: crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT'>> Inserting data into: crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'E:\Google data analytics certificate\Data warwhouse\sql data warehouse my_project\Datasets\source_crm\sales_details.CSV'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',' ,
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR) + ' seconds';
		PRINT'-------------------';


		PRINT'************************************************';
		PRINT'Loading erp tables';
		PRINT'************************************************';
		
		SET @start_time = GETDATE();
		PRINT'>> Truncating: erp_CUST_AZ12';
		TRUNCATE TABLE bronze.erp_CUST_AZ12;

		PRINT'>> Inserting data into: erp_CUST_AZ12';
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'E:\Google data analytics certificate\Data warwhouse\sql data warehouse my_project\Datasets\source_erp\CUST_AZ12.CSV'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',' ,
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR) + ' seconds';
		PRINT'-------------------';

		SET @start_time = GETDATE();
		PRINT'>> Truncating: erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT'>> Inserting data into: erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'E:\Google data analytics certificate\Data warwhouse\sql data warehouse my_project\Datasets\source_erp\loc_a101.CSV'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',' ,
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR) + ' seconds';
		PRINT'-------------------';
	
		SET @start_time = GETDATE();
		PRINT'>> Truncating: erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT'>> Inserting data into: erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'E:\Google data analytics certificate\Data warwhouse\sql data warehouse my_project\Datasets\source_erp\px_cat_g1v2.CSV'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',' ,
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR) + ' seconds';
		PRINT'-------------------';

		SET @batch_start_time = GETDATE();
		PRINT'-------------------';
		PRINT'Bronze layer is completed';
		PRINT '>>Total Load Duration: ' + CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time)AS NVARCHAR) + ' seconds';
		PRINT'-------------------';
	END TRY
	BEGIN CATCH
	PRINT'============================================================'
	PRINT'Error Occured During Loading Bronze Layer'
	PRINT'Error massage'+ ERROR_MESSAGE();
	PRINT'Error massage'+ CAST (ERROR_NUMBER() AS NVARCHAR);
	PRINT'Error massage'+ CAST (ERROR_STATE() AS NVARCHAR);
	PRINT'============================================================'
	END CATCH
END
