/*
===============================================================================
Stored Procedure: Load silver Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure performs ETL (Extract,Transform,Load) process populate to the 'silver' schema from the 'bronze' schema. 
    It performs the following actions:
    - Truncates the silver tables before loading data.
    - inserts transformed and cleansed data from bronze into silver tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver_load_silver as
BEGIN
DECLARE @Start_time DATETIME ,@end_time DATETIME,@start_of_cycle DATETIME ,@end_of_cycle DATETIME;
 BEGIN TRY
	SET @start_of_cycle=GETDATE();
	PRINT'=================================================================';
	PRINT'Loading silver Layer';
	PRINT'=================================================================';

	PRINT'************************************************';
	PRINT'Loading crm tables';
	PRINT'************************************************';


	SET @start_time=GETDATE();
	PRINT '>> Truncating table :silver.crm_cust_info';
	TRUNCATE TABLE  silver.crm_cust_info
	PRINT '>>Inserting data into:silver.crm_cust_info';
	INSERT INTO silver.crm_cust_info
	(cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date)

	SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE 
			 WHEN UPPER(TRIM(cst_marital_status)) ='S' THEN 'Single'
			 WHEN UPPER(TRIM(cst_marital_status)) ='M' THEN 'married'
			 ELSE 'N/A'
		END cst_marital_status,
		CASE 
			 WHEN UPPER(TRIM(cst_gndr)) ='F' THEN 'Female'
			 WHEN UPPER(TRIM(cst_gndr)) ='M' THEN 'male'
			 ELSE 'N/A'
		END cst_gndr,
		cst_create_date
	FROM (
		SELECT *,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date) AS Flag_Last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL )t
	WHERE Flag_Last = 1;    
	SET @end_time=GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR) + ' seconds';
	PRINT'-------------------';


	SET @start_time=GETDATE();
	PRINT '>> Truncating table :silver.crm_prd_info'
	TRUNCATE TABLE  silver.crm_prd_info
	PRINT '>>Inserting data into:silver.crm_prd_info';
	INSERT INTO silver.crm_prd_info
	(prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt)

	SELECT
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id, 
		SUBSTRING(prd_key,7,len(prd_key)) AS prd_key,    
		prd_nm,
		ISNULL(prd_cost,0) as prd_cost,				
		CASE UPPER(TRIM (prd_line))
			 WHEN 'M' THEN 'Mountain'
			 WHEN 'R' THEN 'Road'
			 WHEN 'S' THEN 'other sales'
			 WHEN 'T' THEN 'Touring'
		ELSE'N/A'  
		END AS prd_line ,                    
		CAST (prd_start_dt AS DATE) AS prd_start_dt ,
		CAST (LEAD(prd_start_dt)OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE
		)AS prd_end_dt                         
	FROM bronze.crm_prd_info;
	SET @end_time=GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR) + ' seconds';
	PRINT'-------------------';


	SET @start_time=GETDATE();
	PRINT '>> Truncating table :silver.crm_sales_details'
	TRUNCATE TABLE  silver.crm_sales_details
	PRINT '>>Inserting data into:crm_sales_details';
	INSERT INTO silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price)

	SELECT 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE 
			WHEN sls_order_dt =0 OR LEN (sls_order_dt)<>8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)			   
		END AS sls_order_dt ,
		CASE 
			WHEN sls_ship_dt =0 OR LEN (sls_ship_dt)<>8 THEN NULL     
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)				   
		END AS sls_ship_dt ,
		CASE 
			WHEN sls_due_dt =0 OR LEN (sls_due_dt)<>8 THEN NULL      
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)				   
		END AS sls_due_dt ,
		CASE 
			WHEN sls_sales<=0 OR sls_sales IS NULL OR sls_sales <> sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price) 
			ELSE sls_sales END AS sls_sales,
		sls_quantity,
		CASE 
			WHEN sls_price <=0 OR sls_price IS NULL THEN  ABS(sls_sales / NULLIF(sls_quantity,0))
			ELSE ABS(sls_price) END AS sls_price                 
	FROM bronze.crm_sales_details
	SET @end_time=GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR) + ' seconds';
	PRINT'-------------------';


	PRINT'************************************************';
	PRINT'Loading erp tables';
	PRINT'************************************************';

	SET @start_time=GETDATE();
	PRINT '>> Truncating table :silver.erp_CUST_AZ12'
	TRUNCATE TABLE  silver.erp_CUST_AZ12
	PRINT '>>Inserting data into:erp_CUST_AZ12';
	INSERT INTO [Datawarehouse].[silver].[erp_CUST_AZ12]
	(cid,
	bdate,
	gen)

	SELECT
		CASE 
			 WHEN cid liKe 'NAS%%' THEN SUBSTRING (cid,4,LEN(cid))
			 ELSE cid 
		END AS cid,
		CASE 
			 WHEN bdate> GETDATE() THEN NULL
			 ELSE bdate
		END AS bdate,
		CASE 
			 WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
			 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
			 ELSE 'N/A'
		END AS gen
	FROM [Datawarehouse].[bronze].[erp_CUST_AZ12]
	SET @end_time=GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR) + ' seconds';
	PRINT'-------------------';


	SET @start_time=GETDATE();
	PRINT '>> Truncating table :silver.erp_loc_a101'
	TRUNCATE TABLE  silver.erp_loc_a101
	PRINT '>>Inserting data into:erp_loc_a101';
	INSERT INTO  Datawarehouse.silver.erp_loc_a101
	(cid,
	cntry)

	SELECT
		REPLACE (cid,'-','') as cid,
		CASE 
			WHEN (UPPER(TRIM(cntry)) IN ('US','USA','UNITED STATE','UNITED STATES')) THEN 'United states'
			WHEN (UPPER(TRIM(cntry)) IN ('UK','UNITED KINGDOM')) THEN 'United Kingdom'
			WHEN (UPPER(TRIM(cntry)) IN ('DE','GERMANY')) THEN 'Germany'
			WHEN cntry = '' OR cntry IS NULL THEN 'N/A'
			ELSE TRIM(cntry)
		END AS cntry
	FROM Datawarehouse.bronze.erp_loc_a101
	SET @end_time=GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR) + ' seconds';
	PRINT'-------------------';


	SET @start_time=GETDATE();
	PRINT '>> Truncating table :silver.erp_px_cat_g1v2'
	TRUNCATE TABLE  silver.erp_px_cat_g1v2
	PRINT '>>Inserting data into:erp_px_cat_g1v2';
	INSERT INTO Datawarehouse.silver.erp_px_cat_g1v2
	(id,
	cat,
	subcat,
	maintenance)

	SELECT 
	id,
	cat,
	subcat,
	maintenance
	FROM Datawarehouse.bronze.erp_px_cat_g1v2
	SET @end_time=GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR) + ' seconds';
	PRINT'-------------------';
	SET @end_of_cycle=GETDATE();
	PRINT'-------------------';
	PRINT'silver layer is completed';
	PRINT '>>Total Load Duration: ' + CAST(DATEDIFF(SECOND,@start_of_cycle,@end_of_cycle)AS NVARCHAR) + ' seconds';
	PRINT'-------------------';
 END TRY
 BEGIN CATCH
	PRINT'============================================================'
	PRINT'Error Occured During Loading silver Layer'
	PRINT'Error massage'+ ERROR_MESSAGE();
	PRINT'Error massage'+ CAST (ERROR_NUMBER() AS NVARCHAR);
	PRINT'Error massage'+ CAST (ERROR_STATE() AS NVARCHAR);
	PRINT'============================================================'
 END CATCH
END
