/*
===============================================================================
 Script Name   : bronze_layer.sql
 Purpose       : Creates Bronze Layer tables and loads raw data into them.
                 This represents the raw ingestion layer in a Medallion Architecture.

 ⚠️ WARNING:
 This script drops and recreates all bronze layer tables. Any existing data
 will be lost. Use with caution in production environments.

 Usage:
 - Creates six tables in the [bronze] schema.
 - Loads data from CSV files using BULK INSERT.
 - Logs load duration for each file.
===============================================================================
*/

-- =================================================
-- ============= Build Bronze Layer ================
-- =================================================

-- Drop and create crm_cust_info table
IF OBJECT_ID ('bronze.crm_cust_info','U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info
(
	cst_id				INT,
	cst_key				NVARCHAR(50),
	cst_firstname		NVARCHAR(50),
	cst_lastname		NVARCHAR(50),
	cst_marital_status	NVARCHAR(50),
	cst_gndr			NVARCHAR(50),
	cst_create_date		DATE
);

-- Drop and create crm_prd_info table
IF OBJECT_ID ('bronze.crm_prd_info','U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info
(
	prd_id			INT,
	prd_key			NVARCHAR(50),
	prd_nm			NVARCHAR(50),
	prd_cost		INT,
	prd_line		NVARCHAR(50),
	prd_start_dt	DATETIME,
	prd_end_dt		DATETIME
);

-- Drop and create crm_sales_details table
IF OBJECT_ID ('bronze.crm_sales_details','U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details
(
	sls_ord_num		NVARCHAR(50),
	sls_prd_key		NVARCHAR(50),
	sls_cust_id		INT,
	sls_order_dt	INT,
	sls_ship_dt		INT,
	sls_due_dt		INT,
	sls_sales		INT,
	sls_quantity	INT,
	sls_price		INT
);

-- Drop and create erp_cust_az12 table
IF OBJECT_ID ('bronze.erp_cust_az12','U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12
(
	cid		NVARCHAR(50),
	bdate	DATE,
	gen		NVARCHAR(50)
);

-- Drop and create erp_loc_a101 table
IF OBJECT_ID ('bronze.erp_loc_a101','U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101
(
	cid		NVARCHAR(50),
	cntry	NVARCHAR(50)
);

-- Drop and create erp_px_cat_g1v2 table
IF OBJECT_ID ('bronze.erp_px_cat_g1v2','U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2
(
	id			NVARCHAR(50),
	cat			NVARCHAR(50),
	subcat		NVARCHAR(50),
	maintenance NVARCHAR(50)
);

-- =====================================
-- ==       Load Data to Table        ==
-- =====================================

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME , @batch_end_time DATETIME;

	BEGIN TRY 
		SET @batch_start_time = GETDATE();
		PRINT '===================================';
		PRINT '	    Loading Bronze Layer';
		PRINT '===================================';

		-- ========================
		-- == Load CRM Data ==
		-- ========================
		PRINT '------------------------------';
		PRINT '		Loading CRM Tables';
		PRINT '------------------------------';

		-- crm_cust_info
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info
		PRINT '>> Inserting Data Into: bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'F:\Omar Folder\Education\Data Analysis\Data with Baraa [Youtube Channel]\SQL Course\sql-ultimate-course\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@Start_Time,@End_Time) AS NVARCHAR) + ' seconds';

		-- crm_prd_info
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info
		PRINT '>> Inserting Data Into: bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'F:\Omar Folder\Education\Data Analysis\Data with Baraa [Youtube Channel]\SQL Course\sql-ultimate-course\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@Start_Time,@End_Time) AS NVARCHAR) + ' seconds';

		-- crm_sales_details
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details
		PRINT '>> Inserting Data Into: bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'F:\Omar Folder\Education\Data Analysis\Data with Baraa [Youtube Channel]\SQL Course\sql-ultimate-course\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@Start_Time,@End_Time) AS NVARCHAR) + ' seconds';

		-- ========================
		-- == Load ERP Data ==
		-- ========================
		PRINT '------------------------------';
		PRINT '		Loading ERP Tables';
		PRINT '------------------------------';

		-- erp_cust_az12
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.erp_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12
		PRINT '>> Inserting Data Into: bronze.erp_cust_az12'
		BULK INSERT bronze.erp_cust_az12
		FROM 'F:\F:\Omar Folder\Education\Data Analysis\Data with Baraa [Youtube Channel]\SQL Course\sql-ultimate-course\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@Start_Time,@End_Time) AS NVARCHAR) + ' seconds';

		-- erp_loc_a101
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.erp_loc_a101'
		TRUNCATE TABLE bronze.erp_loc_a101
		PRINT '>> Inserting Data Into: bronze.erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101
		FROM 'F:\Omar Folder\Education\Data Analysis\Data with Baraa [Youtube Channel]\SQL Course\sql-ultimate-course\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@Start_Time,@End_Time) AS NVARCHAR) + ' seconds';

		-- erp_px_cat_g1v2
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2
		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2'
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'F:\Omar Folder\Education\Data Analysis\Data with Baraa [Youtube Channel]\SQL Course\sql-ultimate-course\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@Start_Time,@End_Time) AS NVARCHAR) + ' seconds';

		-- Final Summary
		SET @batch_end_time = GETDATE();
		PRINT '===================================';
		PRINT 'Loading Bronze Layer is Completed';
		PRINT '    - Total Load Duration: ' + CAST(DATEDIFF(second,@Batch_Start_Time,@Batch_End_Time) AS NVARCHAR) + ' second';
		PRINT '===================================';

	END TRY 
	BEGIN CATCH
		-- Error Logging
		PRINT '=========================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE();
		PRINT 'ERROR NUMBER: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR STATE: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================';
	END CATCH
END;
