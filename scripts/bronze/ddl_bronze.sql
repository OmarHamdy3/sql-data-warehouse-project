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
	cst_id		INT,
	cst_key		NVARCHAR(50),
	cst_firstname	NVARCHAR(50),
	cst_lastname	NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr	NVARCHAR(50),
	cst_create_date	DATE
);

-- Drop and create crm_prd_info table
IF OBJECT_ID ('bronze.crm_prd_info','U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info
(
	prd_id		INT,
	prd_key		NVARCHAR(50),
	prd_nm		NVARCHAR(50),
	prd_cost	INT,
	prd_line	NVARCHAR(50),
	prd_start_dt	DATETIME,
	prd_end_dt	DATETIME
);

-- Drop and create crm_sales_details table
IF OBJECT_ID ('bronze.crm_sales_details','U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details
(
	sls_ord_num	NVARCHAR(50),
	sls_prd_key	NVARCHAR(50),
	sls_cust_id	INT,
	sls_order_dt	INT,
	sls_ship_dt	INT,
	sls_due_dt	INT,
	sls_sales	INT,
	sls_quantity	INT,
	sls_price	INT
);

-- Drop and create erp_cust_az12 table
IF OBJECT_ID ('bronze.erp_cust_az12','U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12
(
	cid	NVARCHAR(50),
	bdate	DATE,
	gen	NVARCHAR(50)
);

-- Drop and create erp_loc_a101 table
IF OBJECT_ID ('bronze.erp_loc_a101','U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101
(
	cid	NVARCHAR(50),
	cntry	NVARCHAR(50)
);

-- Drop and create erp_px_cat_g1v2 table
IF OBJECT_ID ('bronze.erp_px_cat_g1v2','U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2
(
	id	NVARCHAR(50),
	cat	NVARCHAR(50),
	subcat	NVARCHAR(50),
	maintenance NVARCHAR(50)
);

